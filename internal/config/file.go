package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type ExtractionRequest struct {
	Topic  string `json:"topic" yaml:"topic"`
	Schema struct {
		Type          string  `json:"type" yaml:"type"`
		DecodingProto *string `json:"decoding_proto,omitempty" yaml:"decoding_proto,omitempty"`
	} `json:"schema" yaml:"schema"`
	Range struct {
		Start uint64 `json:"start" yaml:"start"`
		End   uint64 `json:"end" yaml:"end"`
	} `json:"range" yaml:"range"`
	FilterExpr *string `json:"filter_expr" yaml:"filter_expr"`
	Output     struct {
		Path          string   `json:"path" yaml:"path"`
		Format        string   `json:"format" yaml:"format"`
		Fields        []string `json:"fields" yaml:"fields"`
		Compression   *string  `json:"compression,omitempty" yaml:"compression,omitempty"`
		MaxRecords    uint32   `json:"max_records" yaml:"max_records"`
		ScanCeiling   uint32   `json:"scan_ceiling" yaml:"scan_ceiling"`
		MaxDurationMs uint32   `json:"max_duration_ms" yaml:"max_duration_ms"`
	} `json:"output" yaml:"output"`
}

func ReadExtractionRequest(path string) (*ExtractionRequest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	ext := filepath.Ext(path)

	req := &ExtractionRequest{}
	switch ext {
	case ".json":
		err = json.Unmarshal(data, req)
	case ".yaml", ".yml":
		err = yaml.Unmarshal(data, req)
	default:
		err = fmt.Errorf("invalid configuration file type %q (expected .json, .yaml, or .yml)", ext)
	}
	if err != nil {
		return nil, fmt.Errorf("could not parse config file %q: %w", path, err)
	}
	return req, nil
}
