package types

type CreateBucketRequestPayload struct {
	Name string `json:"name" validate:"required"`
}

type SetRequestPayload struct {
	Bucket string `json:"bucket" validate:"required"`
	Key   string `json:"key" validate:"required"`
	Value string `json:"value" validate:"required"`
}

type GetRequestPayload struct {
    Bucket string `json:"bucket" validate:"required"`
    Key   string `json:"key" validate:"required"`
}

type DeleteRequestPayload struct {
    Bucket string `json:"bucket" validate:"required"`
    Key   string `json:"key" validate:"required"`
}


