package config

import (
	"fmt"

	"github.com/alecthomas/kong"
)

type EndpointInfo struct {
	Host string
	Port uint16
}
type CliArgs struct {
	Config   string       `type:"existingfile" short:"c" help:"Configuration file containing topic, parsing, filtering and output details"`
	Endpoint EndpointInfo `embed:"" prefix:"endpoint." help:"Kafka endpoint configuration"`
	Version  bool
	DryRun   bool
}

func (ca *CliArgs) AfterApply(ctx *kong.Context) error {
	if ca.Version {
		fmt.Printf("kxtract v%s", Version)
		fmt.Println()
		ctx.Exit(0)
	}
	if ca.Config == "" {
		return fmt.Errorf("missing flags: --config=STRING")
	}
	return nil
}

func ParseArgs() (*kong.Context, *CliArgs) {
	cli := &CliArgs{}
	ctx := kong.Parse(
		cli,
		kong.Name("kxtract"),
		kong.Description("Kafka data extractor for forensic precision."),
	)
	return ctx, cli
}
