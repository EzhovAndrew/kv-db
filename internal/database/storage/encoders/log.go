package encoders

type Log struct {
	LSN       uint64
	Command   int
	Arguments []string
}
