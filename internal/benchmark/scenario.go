package benchmark

// Scenario : To define different workloads
type Scenario struct {
	Name          string
	TotalMessages int
	Producers     int
	MessageSize   int // bytes
	Runs          int
}
