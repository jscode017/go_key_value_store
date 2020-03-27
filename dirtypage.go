package go_kvstore

type DirtyPage struct {
	Content []byte
	IsDirty bool
}
