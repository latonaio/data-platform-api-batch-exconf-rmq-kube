package requests

type Batch struct {
	BusinessPartner *int    `json:"BusinessPartner"`
	Batch           *string `json:"Batch"`
}
