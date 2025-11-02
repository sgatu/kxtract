package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/a8m/envsubst"
	"github.com/sgatu/kxtract/internal/helpers"
)

type Request struct {
	Args              *CliArgs           `json:"cli_args"`
	ExtractionRequest *ExtractionRequest `json:"extraction_request"`
}

const (
	KafkaEndpointHost = "KX_KAFKA_ENDPOINT_HOST"
	KafkaEndpointPort = "KX_KAFKA_ENDPOINT_PORT"
)

func getEnv(key string, def string) string {
	e := strings.TrimSpace(os.Getenv(key))
	if e == "" {
		return def
	}
	return e
}

func (r *Request) PrettyPrint() {
	data, err := json.MarshalIndent(r, "", "   ")
	if err == nil {
		fmt.Printf("%s", data)
	}
}

func GetRequest() (*Request, error) {
	ctx, args := ParseArgs()
	if ctx.Error != nil {
		return nil, ctx.Error
	}
	if args.Endpoint.Host == "" {
		args.Endpoint.Host = getEnv(KafkaEndpointHost, "localhost")
	}

	port := getEnv(KafkaEndpointPort, "9092")
	if args.Endpoint.Port == 0 {
		if parsed, err := strconv.ParseUint(port, 10, 16); err == nil {
			args.Endpoint.Port = uint16(parsed)
		}
	}
	exreq, err := ReadExtractionRequest(args.Config)
	if err != nil {
		return nil, err
	}
	if exreq.Output.Path != "" {
		subst, err := envsubst.String(exreq.Output.Path)
		if err != nil {
			return nil, fmt.Errorf("subst err output.path: %w", err)
		}
		exreq.Output.Path = subst
	}
	if exreq.Schema.DecodingProto != nil && *exreq.Schema.DecodingProto != "" {
		subst, err := envsubst.String(*exreq.Schema.DecodingProto)
		if err != nil {
			return nil, fmt.Errorf("subst err schema.decoding_proto: %w", err)
		}
		exreq.Schema.DecodingProto = &subst
	}
	if exreq.Output.Compression == nil {
		exreq.Output.Compression = helpers.Strptr("none")
	}
	req := &Request{
		Args:              args,
		ExtractionRequest: exreq,
	}
	err = validate(req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func validate(r *Request) error {
	if r.Args.Endpoint.Host == "" {
		return fmt.Errorf("invalid config: no host")
	}
	if r.Args.Endpoint.Port == 0 {
		return fmt.Errorf("invalid config: invalid port")
	}
	if r.ExtractionRequest == nil {
		return fmt.Errorf("invalid config: no extraction request")
	}
	if r.ExtractionRequest.Output.Path == "" {
		return fmt.Errorf("invalid config: no extraction output path")
	}
	if len(r.ExtractionRequest.Output.Fields) == 0 {
		return fmt.Errorf("invalid config: no extraction output fields")
	}
	if cmp := r.ExtractionRequest.Output.Compression; cmp != nil && *cmp != "gzip" && *cmp != "none" {
		return fmt.Errorf("invalid config: no extraction output compression invalid %q, only gzip or none allowed", *cmp)
	}
	if frmt := r.ExtractionRequest.Output.Format; frmt != "csv" && frmt != "tsv" && frmt != "ndjson" {
		return fmt.Errorf("invalid config: invalid output format %q (allowed: csv, tsv, ndjson)", frmt)
	}
	if r.ExtractionRequest.Topic == "" {
		return fmt.Errorf("invalid config: topic cannot be empty")
	}
	if r.ExtractionRequest.Range.End != 0 && r.ExtractionRequest.Range.End <= r.ExtractionRequest.Range.Start {
		return fmt.Errorf("invalid config: range end <= start")
	}
	sch := r.ExtractionRequest.Schema.Type
	if sch != "json" && sch != "protobuf" {
		return fmt.Errorf("invalid config: schema type %q not supported (allowed: json, protobuf)", sch)
	}
	if sch == "protobuf" {
		if r.ExtractionRequest.Schema.DecodingProto == nil || *r.ExtractionRequest.Schema.DecodingProto == "" {
			return fmt.Errorf("invalid config: protobuf schema requires a decoding proto file, none provided")
		}
		if _, err := os.Stat(*r.ExtractionRequest.Schema.DecodingProto); err != nil {
			return fmt.Errorf("invalid config: schema decoding proto: %w", err)
		}
	}
	return nil
}
