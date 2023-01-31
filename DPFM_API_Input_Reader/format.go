package dpfm_api_input_reader

import (
	"data-platform-api-batch-exconf-rmq-kube/DPFM_API_Caller/requests"
)

func (sdc *SDC) ConvertToBatch() *requests.Batch {
	data := sdc.Batch
	return &requests.Batch{
		BusinessPartner: data.BusinessPartner,
		Batch:           data.Batch,
	}
}
