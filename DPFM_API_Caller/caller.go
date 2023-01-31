package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-batch-exconf-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-batch-exconf-rmq-kube/DPFM_API_Output_Formatter"
	"encoding/json"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type ExistenceConf struct {
	ctx context.Context
	db  *database.Mysql
	l   *logger.Logger
}

func NewExistenceConf(ctx context.Context, db *database.Mysql, l *logger.Logger) *ExistenceConf {
	return &ExistenceConf{
		ctx: ctx,
		db:  db,
		l:   l,
	}
}

func (e *ExistenceConf) Conf(msg rabbitmq.RabbitmqMessage) interface{} {
	var ret interface{}
	ret = map[string]interface{}{
		"ExistenceConf": false,
	}
	input := make(map[string]interface{})
	err := json.Unmarshal(msg.Raw(), &input)
	if err != nil {
		return ret
	}

	_, ok := input["Batch"]
	if ok {
		input := &dpfm_api_input_reader.SDC{}
		err = json.Unmarshal(msg.Raw(), input)
		ret = e.confBatch(input)
		goto endProcess
	}

	err = xerrors.Errorf("can not get exconf check target")
endProcess:
	if err != nil {
		e.l.Error(err)
	}
	return ret
}

func (e *ExistenceConf) confBatch(input *dpfm_api_input_reader.SDC) *dpfm_api_output_formatter.Batch {
	exconf := dpfm_api_output_formatter.Batch{
		ExistenceConf: false,
	}
	if input.Batch.BusinessPartner == nil {
		return &exconf
	}
	if input.Batch.Product == nil {
		return &exconf
	}
	if input.Batch.Plant == nil {
		return &exconf
	}
	if input.Batch.Batch == nil {
		return &exconf
	}
	exconf = dpfm_api_output_formatter.Batch{
		BusinessPartner: *input.Batch.BusinessPartner,
		Product:         *input.Batch.Product,
		Plant:           *input.Batch.Plant,
		Batch:           *input.Batch.Batch,
		ExistenceConf:   false,
	}

	rows, err := e.db.Query(
		`SELECT BusinessPartner, Product, Plant, Batch 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_batch_master_record_batch_data 
		WHERE (BusinessPartner, Product, Plant, Batch) = (?, ?, ?, ?);`, exconf.BusinessPartner, exconf.Product, exconf.Plant, exconf.Batch,
	)
	if err != nil {
		e.l.Error(err)
		return &exconf
	}
	defer rows.Close()

	exconf.ExistenceConf = rows.Next()
	return &exconf
}
