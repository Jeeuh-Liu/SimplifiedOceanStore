module puddlestore

go 1.13

require (
	github.com/google/uuid v1.2.0
	github.com/hashicorp/go-msgpack v1.1.5
	github.com/samuel/go-zookeeper v0.0.0-20201211165307-7117e9ea2414
	tapestry v0.0.0-00010101000000-000000000000
)

// Replace this
replace tapestry => /path/to/your/tapestry/implementation/root/folder