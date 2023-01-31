# data-platform-api-batch-exconf-rmq-kube
data-platform-api-batch-exconf-rmq-kube は、データ連携基盤において、API で ロットデータの存在性チェックを行うためのマイクロサービスです。

## 動作環境
・ OS: LinuxOS  
・ CPU: ARM/AMD/Intel  

## 存在確認先テーブル名
以下のsqlファイルに対して、ロットデータの存在確認が行われます。

* data-platform-batch-master-record-sql-batch-data.sql（データ連携基盤 ロットデータ - ロットデータ）

## caller.go による存在性確認
Input で取得されたファイルに基づいて、caller.go で、 API がコールされます。
caller.go の 以下の箇所が、指定された API をコールするソースコードです。

```
func (e *ExistenceConf) Conf(input *dpfm_api_input_reader.SDC) *dpfm_api_output_formatter.Batch {
	businessPartner := *input.Batch.BusinessPartner
	batch := *input.Batch.Batch
	notKeyExistence := make([]dpfm_api_output_formatter.Batch, 0, 1)
	KeyExistence := make([]dpfm_api_output_formatter.Batch, 0, 1)

	existData := &dpfm_api_output_formatter.Batch{
		BusinessPartner: businessPartner,
		Batch:           batch,
		ExistenceConf:   false,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !e.confBatch(businessPartner, batch) {
			notKeyExistence = append(
				notKeyExistence,
				dpfm_api_output_formatter.Batch{businessPartner, batch, false},
			)
			return
		}
		KeyExistence = append(KeyExistence, dpfm_api_output_formatter.Batch{businessPartner, batch, true})
	}()

	wg.Wait()

	if len(KeyExistence) == 0 {
		return existData
	}
	if len(notKeyExistence) > 0 {
		return existData
	}

	existData.ExistenceConf = true
	return existData
}
```

## Input
data-platform-api-batch-exconf-rmq-kube では、以下のInputファイルをRabbitMQからJSON形式で受け取ります。  

```
{
	"connection_key": "request",
	"result": true,
	"redis_key": "abcdefg",
	"api_status_code": 200,
	"runtime_session_id": "boi9ar543dg91ipdnspi099u231280ab0v8af0ew",
	"business_partner": 201,
	"filepath": "/var/lib/aion/Data/rededge_sdc/abcdef.json",
	"service_label": "ORDERS",
	"Batch": {
		"BusinessPartner": "101",
		"Batch": "AB01"
	},
	"api_schema": "DPFMOrdersCreates",
	"accepter": ["All"],
	"order_id": null,
	"deleted": false
}

```

## Output
data-platform-api-batch-exconf-rmq-kube では、[golang-logging-library-for-data-platform](https://github.com/latonaio/golang-logging-library-for-data-platform) により、Output として、RabbitMQ へのメッセージを JSON 形式で出力します。ロットデータの対象値が存在する場合 true、存在しない場合 false、を返します。"cursor" ～ "time"は、golang-logging-library-for-data-platform による 定型フォーマットの出力結果です。

```
{
	"connection_key": "request",
	"result": true,
	"redis_key": "abcdefg",
	"filepath": "/var/lib/aion/Data/rededge_sdc/abcdef.json",
	"api_status_code": 200,
	"runtime_session_id": "boi9ar543dg91ipdnspi099u231280ab0v8af0ew",
	"business_partner": 201,
	"service_label": "ORDERS",
	"Batch": {
		"BusinessPartner": 101,
		"Batch": "AB01",
		"ExistenceConf": true
	},
	"api_schema": "DPFMOrdersCreates",
	"accepter": [
		"All"
	],
	"order_id": null,
	"deleted": false
}
```