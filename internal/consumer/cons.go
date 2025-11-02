package consumer

import (
	"context"
	"fmt"

	"github.com/sgatu/kxtract/internal/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	client       *kgo.Client
	consumeRange map[int32]PartitionRange
	topic        string
	onMsg        onMessage
}
type PartitionRange struct {
	start int64
	end   int64
}
type onMessage func([]byte)

func NewConsumer(cfg *config.ExtractionRequest, onMsg onMessage) (*Consumer, error) {
	startOffsets, offsetRange, err := getOffsets(cfg)
	if err != nil {
		return nil, err
	}
	opts := make([]kgo.Opt, 0)
	opts = append(opts, kgo.DisableAutoCommit())
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	c.AddConsumePartitions(startOffsets)
	return &Consumer{client: c, consumeRange: offsetRange, onMsg: onMsg, topic: cfg.Topic}, err
}

func (c *Consumer) Read(ctx context.Context) {
	for {
		fetches := c.client.PollRecords(ctx, 1000)
		if err := fetches.Err(); err != nil {
			return
		}
		removePartitions := map[string][]int32{c.topic: {}}
		fetches.EachRecord(func(r *kgo.Record) {
			rng, ok := c.consumeRange[r.Partition]
			if !ok {
				return
			}
			if r.Offset > rng.end {
				removePartitions[c.topic] = append(removePartitions[c.topic], r.Partition)
				delete(c.consumeRange, r.Partition)
				return
			}
			c.onMsg(r.Value)
			if r.Offset == rng.end {
				removePartitions[c.topic] = append(removePartitions[c.topic], r.Partition)
				delete(c.consumeRange, r.Partition)
			}
		})
		if len(removePartitions[c.topic]) > 0 {
			c.client.RemoveConsumePartitions(removePartitions)
		}
		if len(c.consumeRange) == 0 {
			break
		}
	}
}

func getOffsets(cfg *config.ExtractionRequest) (map[string]map[int32]kgo.Offset, map[int32]PartitionRange, error) {
	client, err := kgo.NewClient(kgo.DisableAutoCommit())
	if err != nil {
		return nil, nil, err
	}
	defer client.Close()
	aclient := kadm.NewClient(client)
	partitionOffsets := make(map[int32]PartitionRange)
	okOffsets := make(map[int32]PartitionRange)
	startOffsets, err := aclient.ListStartOffsets(context.Background(), cfg.Topic)
	if err != nil {
		return nil, nil, err
	}
	endOffsets, err := aclient.ListEndOffsets(context.Background(), cfg.Topic)
	if err != nil {
		return nil, nil, err
	}
	for i, soff := range startOffsets.KOffsets()[cfg.Topic] {
		eoff := endOffsets.KOffsets()[cfg.Topic][i]
		if soff.EpochOffset().Offset != eoff.EpochOffset().Offset {
			partitionOffsets[i] = PartitionRange{start: soff.EpochOffset().Offset, end: eoff.EpochOffset().Offset}
		}
	}
	if len(partitionOffsets) == 0 {
		return nil, nil, fmt.Errorf("no data in partitions")
	}

	offsetsAfterStart, err := aclient.ListOffsetsAfterMilli(context.Background(), int64(cfg.Range.Start), cfg.Topic)
	if err != nil {
		return nil, nil, err
	}
	offsetsAfterEnd, err := aclient.ListOffsetsAfterMilli(context.Background(), int64(cfg.Range.End), cfg.Topic)
	if err != nil {
		return nil, nil, err
	}
	oS := offsetsAfterStart.KOffsets()[cfg.Topic]
	oE := offsetsAfterEnd.KOffsets()[cfg.Topic]
	setOffsets := make(map[string]map[int32]kgo.Offset)
	setOffsets[cfg.Topic] = make(map[int32]kgo.Offset)
	for i := range partitionOffsets {
		from := max(partitionOffsets[i].start, oS[i].EpochOffset().Offset)
		to := min(partitionOffsets[i].end, oE[i].EpochOffset().Offset)
		if from < to {
			okOffsets[i] = PartitionRange{
				start: from,
				end:   to,
			}
			setOffsets[cfg.Topic][i] = kgo.NewOffset().At(from)
		}
	}
	if len(okOffsets) == 0 {
		return nil, nil, fmt.Errorf("no data in range")
	}
	return setOffsets, okOffsets, nil
}
