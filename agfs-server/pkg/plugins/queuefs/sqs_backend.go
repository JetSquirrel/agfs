package queuefs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	pluginConfig "github.com/c4pt0r/agfs/agfs-server/pkg/plugin/config"
	log "github.com/sirupsen/logrus"
)

const (
	sqsAttrMessageID       = "ID"
	sqsAttrMessageTS       = "Timestamp"
	sqsMessageTypeValue    = "queuefs"
	sqsDefaultWaitSeconds  = int32(10)
	sqsDefaultVisibility   = int32(30)
	sqsDefaultMaxReceive   = int32(1)
	sqsDefaultQueueTimeout = 15 * time.Second
	sqsDeletedQueueTTL     = 2 * time.Minute
)

// SQSClient wraps AWS SQS SDK operations.
type SQSClient struct {
	client *sqs.Client
	region string
}

// NewSQSClient creates a new SQS client from plugin config.
func NewSQSClient(ctx context.Context, cfg map[string]interface{}) (*SQSClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	region := pluginConfig.GetStringConfig(cfg, "region", "")
	if region == "" {
		return nil, fmt.Errorf("missing required config: region")
	}

	// Build AWS SDK config options
	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(region),
	}

	accessKeyID := pluginConfig.GetStringConfig(cfg, "access_key_id", "")
	secretAccessKey := pluginConfig.GetStringConfig(cfg, "secret_access_key", "")
	sessionToken := pluginConfig.GetStringConfig(cfg, "session_token", "")
	if accessKeyID != "" && secretAccessKey != "" {
		opts = append(opts, awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken),
		))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	clientOpts := make([]func(*sqs.Options), 0, 1)
	endpoint := pluginConfig.GetStringConfig(cfg, "endpoint", "")
	if endpoint != "" {
		clientOpts = append(clientOpts, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	return &SQSClient{
		client: sqs.NewFromConfig(awsCfg, clientOpts...),
		region: region,
	}, nil
}

// EnsureQueueURL returns a queue URL, creating the queue if it does not exist.
func (c *SQSClient) EnsureQueueURL(ctx context.Context, queueName string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueName == "" {
		return "", fmt.Errorf("queue name cannot be empty")
	}

	url, err := c.GetQueueURL(ctx, queueName)
	if err == nil {
		return url, nil
	}

	if !isQueueDoesNotExistError(err) {
		return "", fmt.Errorf("failed to get queue URL for %q: %w", queueName, err)
	}

	if err := c.CreateQueue(ctx, queueName); err != nil {
		return "", fmt.Errorf("failed to create queue %q: %w", queueName, err)
	}

	url, err = c.GetQueueURL(ctx, queueName)
	if err != nil {
		return "", fmt.Errorf("failed to get queue URL after create for %q: %w", queueName, err)
	}
	return url, nil
}

// CreateQueue creates a standard SQS queue.
func (c *SQSClient) CreateQueue(ctx context.Context, queueName string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueName == "" {
		return fmt.Errorf("queue name cannot be empty")
	}

	_, err := c.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		// QueueNameExists is okay in idempotent create semantics.
		var exists *types.QueueNameExists
		if errors.As(err, &exists) {
			return nil
		}
		return err
	}
	return nil
}

// DeleteQueue deletes a queue by URL.
func (c *SQSClient) DeleteQueue(ctx context.Context, queueURL string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueURL == "" {
		return fmt.Errorf("queue URL cannot be empty")
	}

	_, err := c.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	return err
}

// PurgeQueue removes all messages from queue.
func (c *SQSClient) PurgeQueue(ctx context.Context, queueURL string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueURL == "" {
		return fmt.Errorf("queue URL cannot be empty")
	}

	_, err := c.client.PurgeQueue(ctx, &sqs.PurgeQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	return err
}

// GetQueueURL retrieves queue URL by queue name.
func (c *SQSClient) GetQueueURL(ctx context.Context, queueName string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueName == "" {
		return "", fmt.Errorf("queue name cannot be empty")
	}

	out, err := c.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", err
	}
	if out.QueueUrl == nil || *out.QueueUrl == "" {
		return "", fmt.Errorf("empty queue URL returned for queue %q", queueName)
	}
	return *out.QueueUrl, nil
}

// ListQueues lists queue names and supports optional name prefix.
func (c *SQSClient) ListQueues(ctx context.Context, prefix string) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	result := make([]string, 0, 16)
	var nextToken *string

	for {
		out, err := c.client.ListQueues(ctx, &sqs.ListQueuesInput{
			QueueNamePrefix: aws.String(prefix),
			NextToken:       nextToken,
		})
		if err != nil {
			return nil, err
		}

		for _, u := range out.QueueUrls {
			name := queueNameFromURL(u)
			if name != "" {
				result = append(result, name)
			}
		}

		if out.NextToken == nil || *out.NextToken == "" {
			break
		}
		nextToken = out.NextToken
	}
	sort.Strings(result)
	return result, nil
}

// ApproximateNumberOfMessages returns queue approximate visible message count.
func (c *SQSClient) ApproximateNumberOfMessages(ctx context.Context, queueURL string) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueURL == "" {
		return 0, fmt.Errorf("queue URL cannot be empty")
	}

	out, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		return 0, err
	}

	val := out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	if val == "" {
		return 0, nil
	}

	var n int
	_, scanErr := fmt.Sscanf(val, "%d", &n)
	if scanErr != nil {
		return 0, fmt.Errorf("invalid ApproximateNumberOfMessages value %q: %w", val, scanErr)
	}
	return n, nil
}

// SendMessage sends one queuefs message to queue URL.
func (c *SQSClient) SendMessage(ctx context.Context, queueURL string, msg QueueMessage) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueURL == "" {
		return fmt.Errorf("queue URL cannot be empty")
	}
	if msg.Data == "" {
		return fmt.Errorf("message body cannot be empty")
	}

	ts := msg.Timestamp.UTC().Format(time.RFC3339Nano)
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(msg.Data),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Type": {
				DataType:    aws.String("String"),
				StringValue: aws.String(sqsMessageTypeValue),
			},
			sqsAttrMessageID: {
				DataType:    aws.String("String"),
				StringValue: aws.String(msg.ID),
			},
			sqsAttrMessageTS: {
				DataType:    aws.String("String"),
				StringValue: aws.String(ts),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("send message failed: %w", err)
	}
	return nil
}

// ReceiveMessages receives up to maxMessages from queue URL.
// If autoDelete=true, it deletes received messages before return (dequeue behavior).
func (c *SQSClient) ReceiveMessages(ctx context.Context, queueURL string, maxMessages int32, autoDelete bool) ([]QueueMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueURL == "" {
		return nil, fmt.Errorf("queue URL cannot be empty")
	}
	if maxMessages < 1 {
		maxMessages = 1
	}
	if maxMessages > 10 {
		maxMessages = 10 // SQS max per receive
	}

	visibilityTimeout := sqsDefaultVisibility
	if !autoDelete {
		visibilityTimeout = 0
	}

	out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   maxMessages,
		VisibilityTimeout:     visibilityTimeout,
		WaitTimeSeconds:       sqsDefaultWaitSeconds,
		MessageAttributeNames: []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameSentTimestamp,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("receive message failed: %w", err)
	}

	messages := make([]QueueMessage, 0, len(out.Messages))
	for _, m := range out.Messages {
		qm, convErr := fromSQSMessage(m)
		if convErr != nil {
			return nil, convErr
		}
		messages = append(messages, qm)
	}

	if autoDelete && len(out.Messages) > 0 {
		entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(out.Messages))
		for i, m := range out.Messages {
			if m.ReceiptHandle == nil || *m.ReceiptHandle == "" {
				continue
			}
			entries = append(entries, types.DeleteMessageBatchRequestEntry{
				Id:            aws.String(fmt.Sprintf("d-%d", i)),
				ReceiptHandle: m.ReceiptHandle,
			})
		}
		if len(entries) > 0 {
			delOut, delErr := c.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(queueURL),
				Entries:  entries,
			})
			if delErr != nil {
				return nil, fmt.Errorf("delete received messages failed: %w", delErr)
			}
			if len(delOut.Failed) > 0 {
				return nil, fmt.Errorf("delete received messages partially failed: %d failed", len(delOut.Failed))
			}
		}
	}

	return messages, nil
}

// QueueExists checks if queue exists.
func (c *SQSClient) QueueExists(ctx context.Context, queueName string) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := c.GetQueueURL(ctx, queueName)
	if err == nil {
		return true, nil
	}
	if isQueueDoesNotExistError(err) {
		return false, nil
	}
	return false, err
}

// SQSBackend implements QueueBackend on AWS SQS.
type SQSBackend struct {
	sqs         *SQSClient
	backendType string

	cache   map[string]string
	deleted map[string]time.Time
	cacheMu sync.RWMutex
}

// NewSQSBackend creates a new SQS backend.
func NewSQSBackend() *SQSBackend {
	return &SQSBackend{
		backendType: "sqs",
		cache:       make(map[string]string),
		deleted:     make(map[string]time.Time),
	}
}

// Initialize initializes SQS backend.
func (b *SQSBackend) Initialize(config map[string]interface{}) error {
	client, err := NewSQSClient(context.Background(), config)
	if err != nil {
		return err
	}
	b.sqs = client
	b.backendType = "sqs"
	if b.cache == nil {
		b.cache = make(map[string]string)
	}
	if b.deleted == nil {
		b.deleted = make(map[string]time.Time)
	}
	log.Infof("[queuefs] initialized with backend: sqs (region: %s)", client.region)
	return nil
}

// Close closes backend.
func (b *SQSBackend) Close() error {
	// AWS SDK v2 client doesn't require explicit close.
	return nil
}

// GetType returns backend type.
func (b *SQSBackend) GetType() string {
	return "sqs"
}

func (b *SQSBackend) getQueueURL(ctx context.Context, queueName string, createIfMissing bool) (string, error) {
	if queueName == "" {
		return "", fmt.Errorf("queue name cannot be empty")
	}
	if strings.Contains(queueName, "/") {
		return "", fmt.Errorf("nested queue name %q is not supported by sqs backend", queueName)
	}

	b.cacheMu.RLock()
	if u, ok := b.cache[queueName]; ok && u != "" {
		b.cacheMu.RUnlock()
		return u, nil
	}
	b.cacheMu.RUnlock()

	var (
		url string
		err error
	)
	if createIfMissing {
		url, err = b.sqs.EnsureQueueURL(ctx, queueName)
	} else {
		url, err = b.sqs.GetQueueURL(ctx, queueName)
	}
	if err != nil {
		return "", err
	}

	b.cacheMu.Lock()
	b.cache[queueName] = url
	delete(b.deleted, queueName)
	b.cacheMu.Unlock()
	return url, nil
}

// Enqueue adds one message.
func (b *SQSBackend) Enqueue(queueName string, msg QueueMessage) error {
	if b.sqs == nil {
		return fmt.Errorf("sqs backend not initialized")
	}
	if msg.ID == "" {
		msg.ID = fmt.Sprintf("sqs-%d", time.Now().UnixNano())
	}
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now().UTC()
	}

	ctx, cancel := context.WithTimeout(context.Background(), sqsDefaultQueueTimeout)
	defer cancel()

	url, err := b.getQueueURL(ctx, queueName, true)
	if err != nil {
		return fmt.Errorf("enqueue get queue url failed: %w", err)
	}
	return b.sqs.SendMessage(ctx, url, msg)
}

// Dequeue removes and returns the first message.
func (b *SQSBackend) Dequeue(queueName string) (QueueMessage, bool, error) {
	if b.sqs == nil {
		return QueueMessage{}, false, fmt.Errorf("sqs backend not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), sqsDefaultQueueTimeout)
	defer cancel()

	url, err := b.getQueueURL(ctx, queueName, false)
	if err != nil {
		if isQueueDoesNotExistError(err) {
			return QueueMessage{}, false, nil
		}
		return QueueMessage{}, false, fmt.Errorf("dequeue get queue url failed: %w", err)
	}

	msgs, err := b.sqs.ReceiveMessages(ctx, url, sqsDefaultMaxReceive, true)
	if err != nil {
		return QueueMessage{}, false, err
	}
	if len(msgs) == 0 {
		return QueueMessage{}, false, nil
	}
	return msgs[0], true, nil
}

// Peek returns first message without deleting it.
func (b *SQSBackend) Peek(queueName string) (QueueMessage, bool, error) {
	if b.sqs == nil {
		return QueueMessage{}, false, fmt.Errorf("sqs backend not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), sqsDefaultQueueTimeout)
	defer cancel()

	url, err := b.getQueueURL(ctx, queueName, false)
	if err != nil {
		if isQueueDoesNotExistError(err) {
			return QueueMessage{}, false, nil
		}
		return QueueMessage{}, false, fmt.Errorf("peek get queue url failed: %w", err)
	}

	msgs, err := b.sqs.ReceiveMessages(ctx, url, sqsDefaultMaxReceive, false)
	if err != nil {
		return QueueMessage{}, false, err
	}
	if len(msgs) == 0 {
		return QueueMessage{}, false, nil
	}
	return msgs[0], true, nil
}

// Size returns approximate queue size.
func (b *SQSBackend) Size(queueName string) (int, error) {
	if b.sqs == nil {
		return 0, fmt.Errorf("sqs backend not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), sqsDefaultQueueTimeout)
	defer cancel()

	url, err := b.getQueueURL(ctx, queueName, false)
	if err != nil {
		if isQueueDoesNotExistError(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("size get queue url failed: %w", err)
	}

	return b.sqs.ApproximateNumberOfMessages(ctx, url)
}

// Clear purges all messages from a queue.
func (b *SQSBackend) Clear(queueName string) error {
	if b.sqs == nil {
		return fmt.Errorf("sqs backend not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), sqsDefaultQueueTimeout)
	defer cancel()

	url, err := b.getQueueURL(ctx, queueName, false)
	if err != nil {
		if isQueueDoesNotExistError(err) {
			return nil
		}
		return fmt.Errorf("clear get queue url failed: %w", err)
	}
	return b.sqs.PurgeQueue(ctx, url)
}

// ListQueues returns queue names filtered by prefix.
func (b *SQSBackend) ListQueues(prefix string) ([]string, error) {
	if b.sqs == nil {
		return nil, fmt.Errorf("sqs backend not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqsDefaultQueueTimeout)
	defer cancel()

	queues, err := b.sqs.ListQueues(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list queues failed: %w", err)
	}

	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()

	now := time.Now()
	for q, expireAt := range b.deleted {
		if now.After(expireAt) {
			delete(b.deleted, q)
		}
	}

	filtered := make([]string, 0, len(queues))
	seen := make(map[string]struct{}, len(queues))
	for _, q := range queues {
		if _, deleted := b.deleted[q]; deleted {
			continue
		}
		filtered = append(filtered, q)
		seen[q] = struct{}{}
	}

	for q := range b.cache {
		if _, deleted := b.deleted[q]; deleted {
			continue
		}
		if prefix == "" || strings.HasPrefix(q, prefix) {
			if _, ok := seen[q]; !ok {
				filtered = append(filtered, q)
				seen[q] = struct{}{}
			}
		}
	}

	sort.Strings(filtered)
	return filtered, nil
}

// GetLastEnqueueTime is not directly queryable in SQS; returns zero time.
func (b *SQSBackend) GetLastEnqueueTime(queueName string) (time.Time, error) {
	// SQS does not expose "latest enqueue timestamp" at queue level.
	// Returning zero value keeps interface compatibility.
	_ = queueName
	return time.Time{}, nil
}

// RemoveQueue deletes queue and nested queues.
func (b *SQSBackend) RemoveQueue(queueName string) error {
	if b.sqs == nil {
		return fmt.Errorf("sqs backend not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if queueName == "" {
		qs, err := b.sqs.ListQueues(ctx, "")
		if err != nil {
			return err
		}
		for _, qn := range qs {
			if err := b.removeOneQueue(ctx, qn); err != nil {
				log.Warnf("[queuefs] failed to remove queue %q: %v", qn, err)
			}
		}
		return nil
	}
	if strings.Contains(queueName, "/") {
		return fmt.Errorf("nested queue name %q is not supported by sqs backend", queueName)
	}
	return b.removeOneQueue(ctx, queueName)
}

func (b *SQSBackend) removeOneQueue(ctx context.Context, queueName string) error {
	url, err := b.getQueueURL(ctx, queueName, false)
	if err != nil {
		if isQueueDoesNotExistError(err) {
			return nil
		}
		return err
	}

	if err := b.sqs.DeleteQueue(ctx, url); err != nil {
		if isQueueDoesNotExistError(err) {
			b.cacheMu.Lock()
			delete(b.cache, queueName)
			b.deleted[queueName] = time.Now().Add(sqsDeletedQueueTTL)
			b.cacheMu.Unlock()
			return nil
		}
		return err
	}
	b.cacheMu.Lock()
	delete(b.cache, queueName)
	b.deleted[queueName] = time.Now().Add(sqsDeletedQueueTTL)
	b.cacheMu.Unlock()
	return nil
}

// CreateQueue creates queue if it doesn't exist.
func (b *SQSBackend) CreateQueue(queueName string) error {
	if b.sqs == nil {
		return fmt.Errorf("sqs backend not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqsDefaultQueueTimeout)
	defer cancel()

	_, err := b.getQueueURL(ctx, queueName, true)
	return err
}

// QueueExists checks queue existence.
func (b *SQSBackend) QueueExists(queueName string) (bool, error) {
	if b.sqs == nil {
		return false, fmt.Errorf("sqs backend not initialized")
	}
	if strings.Contains(queueName, "/") {
		return false, fmt.Errorf("nested queue name %q is not supported by sqs backend", queueName)
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqsDefaultQueueTimeout)
	defer cancel()

	return b.sqs.QueueExists(ctx, queueName)
}

func fromSQSMessage(m types.Message) (QueueMessage, error) {
	var msg QueueMessage

	if m.Body == nil {
		msg.Data = ""
	} else {
		msg.Data = aws.ToString(m.Body)
	}

	// ID
	if attr, ok := m.MessageAttributes[sqsAttrMessageID]; ok && attr.StringValue != nil {
		msg.ID = *attr.StringValue
	} else if m.MessageId != nil {
		msg.ID = *m.MessageId
	}

	// Timestamp
	if attr, ok := m.MessageAttributes[sqsAttrMessageTS]; ok && attr.StringValue != nil {
		if t, err := time.Parse(time.RFC3339Nano, *attr.StringValue); err == nil {
			msg.Timestamp = t
		}
	}
	if msg.Timestamp.IsZero() {
		if sentTS, ok := m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)]; ok && sentTS != "" {
			var ms int64
			if _, err := fmt.Sscanf(sentTS, "%d", &ms); err == nil {
				msg.Timestamp = time.Unix(0, ms*int64(time.Millisecond)).UTC()
			}
		}
	}
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now().UTC()
	}

	return msg, nil
}

func queueNameFromURL(u string) string {
	u = strings.TrimSpace(u)
	if u == "" {
		return ""
	}
	parts := strings.Split(u, "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func isQueueDoesNotExistError(err error) bool {
	var notFound *types.QueueDoesNotExist
	if errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist", "NonExistentQueue":
			return true
		}
	}
	return false
}
