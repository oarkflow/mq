package sio

type Adapter interface {
	Init()
	Shutdown() error
	BroadcastToBackend(*BroadcastMsg)
	RoomcastToBackend(*RoomMsg)
	BroadcastFromBackend(b chan<- *BroadcastMsg)
	RoomcastFromBackend(r chan<- *RoomMsg)
}
