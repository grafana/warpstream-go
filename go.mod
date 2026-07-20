module github.com/grafana/warpstream-go

go 1.25.10

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/klauspost/compress v1.19.0
	github.com/prometheus/client_golang v1.23.3-0.20260305100053-48a6770e980b
	github.com/prometheus/client_model v0.6.2
	github.com/stretchr/testify v1.11.1
	github.com/twmb/franz-go v1.21.5
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260515175617-8268a5d078c0
	github.com/twmb/franz-go/pkg/kmsg v1.13.1
	github.com/twmb/franz-go/plugin/kprom v1.4.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/common v0.70.0 // indirect
	github.com/prometheus/procfs v0.21.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// TODO(temporary): drop this replace and bump to a released franz-go once the
// NewRecordAttrs constructor lands upstream (github.com/twmb/franz-go#1369).
replace github.com/twmb/franz-go => github.com/pracucci/franz-go v0.0.0-20260713145905-fb26a76eebf7
