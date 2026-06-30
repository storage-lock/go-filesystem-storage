module github.com/storage-lock/go-filesystem-storage

go 1.19

require (
	github.com/golang-infrastructure/go-iterator v0.0.0-20230524171120-56988a9b127c
	github.com/storage-lock/go-storage v0.0.2
	github.com/storage-lock/go-storage-lock v0.0.0-20230804163528-6e105948ac37
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang-infrastructure/go-goroutine-id v0.0.0-20230917075314-f1af231c0b06 // indirect
	github.com/golang-infrastructure/go-pointer v0.0.5 // indirect
	github.com/golang-infrastructure/go-reflect-utils v0.0.0-20221130143747-965ef2eb09c3 // indirect
	github.com/google/uuid v1.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/storage-lock/go-events v0.0.3 // indirect
	github.com/storage-lock/go-storage-events v0.0.5 // indirect
	github.com/storage-lock/go-utils v0.0.2 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/storage-lock/go-storage => ../go-storage

replace github.com/storage-lock/go-sql-based-storage => ../go-sql-based-storage

replace github.com/storage-lock/go-storage-lock => ../go-storage-lock
