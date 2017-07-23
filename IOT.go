package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/op/go-logging"
)

//type ServicesChaincode struct {
//}

type IOT struct {
	drr DRR
	cl  CL
}

type Contract struct {
	ContractNo string `json:"contractNo"`
}

type IOTJSON struct {
	iothub      string `json:"iothub"`
	deviceid    string `json:"deviceid"`
	ambientTemp string `json:"ambientTemp"`
	objectTemp  string `json:"objectTemp"`
	humidity    string `json:"humidity"`
	pressure    string `json:"pressure"`
	altitude    string `json:"altitude"`
	accelX      string `json:"accelX"`
	accelY      string `json:"accelY"`
	accelZ      string `json:"accelZ"`
	gyroX       string `json:"gyroX"`
	gyroY       string `json:"gyroY"`
	gyroZ       string `json:"gyroZ"`
	magX        string `json:"magX"`
	magY        string `json:"magY"`
	magZ        string `json:"magZ"`
	light       string `json:"light"`
	time        string `json:"time"`
}

var myLoggerIOT = logging.MustGetLogger("IOT-Services")

func (t *IOT) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	// Check if table already exists
	_, err := stub.GetTable("IOTTable")
	if err == nil {
		// Table already exists; do not recreate
		return nil, nil
	}

	// Create IOT Table
	err = stub.CreateTable("IOTTable", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "Type", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "ContractNo", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "iothub", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "deviceid", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "ambientTemp", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "objectTemp", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "humidity", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "pressure", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "altitude", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "accelX", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "accelY", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "accelZ", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "gyroX", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "gyroY", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "gyroZ", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "magX", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "magY", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "magZ", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "light", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "time", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	if err != nil {
		return nil, errors.New("Failed creating IOTTable.")
	}
	return nil, nil
}
/*
func (t *IOT) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	myLogger.Debugf("-------------------------------------------------------------------")
	myLogger.Debugf("Function : ", function)
	myLogger.Debugf("args : ", args)

	if function == "SubmitDoc" {
		return t.SubmitDoc(stub, args)
	}
	return nil, errors.New("Received unknown function invocation")
}
*/

//SubmitDoc () inserts a new row in the table
func (t *IOT) SubmitDoc(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Submit IOT Data")
	myLoggerIOT.Debugf("args : ", args)
	
	if len(args) != 18 {
		return nil, errors.New("Incorrect number of arguments. Expecting 18")
	}
	
	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("No. of Arguments Passed")
	
	deviceid := args[1]

	// to get contract id from device id
	var contractid Contract
	
	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Just Before GetContractNo")	
	
	b1, err := t.drr.GetContractNo(stub, []string{deviceid})

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Error Just after GetContractNo", err)
	
	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Just after GetContractNo")
	
	contractid.ContractNo = string(b1)
	
	if b1 == nil {
		myLoggerIOT.Debugf("-------------------------------------------------------------------")
		myLoggerIOT.Debugf("Before B1 = NIL")
		return nil, errors.New("ContractNo Not Found")
	}
	
	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("GetContractNo : ", b1)

	
	ContractNo := contractid.ContractNo
	iothub := args[0]
	ambientTemp := args[2]
	objectTemp := args[3]
	humidity := args[4]
	pressure := args[5]
	altitude := args[6]
	accelX := args[7]
	accelY := args[8]
	accelZ := args[9]
	gyroX := args[10]
	gyroY := args[11]
	gyroZ := args[12]
	magX := args[13]
	magY := args[14]
	magZ := args[15]
	light := args[16]
	time := args[17]

	// Insert a row
	ok, err := stub.InsertRow("IOTTable", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: "IOT"}},
			&shim.Column{Value: &shim.Column_String_{String_: ContractNo}},
			&shim.Column{Value: &shim.Column_String_{String_: iothub}},
			&shim.Column{Value: &shim.Column_String_{String_: deviceid}},
			&shim.Column{Value: &shim.Column_String_{String_: ambientTemp}},
			&shim.Column{Value: &shim.Column_String_{String_: objectTemp}},
			&shim.Column{Value: &shim.Column_String_{String_: humidity}},
			&shim.Column{Value: &shim.Column_String_{String_: pressure}},
			&shim.Column{Value: &shim.Column_String_{String_: altitude}},
			&shim.Column{Value: &shim.Column_String_{String_: accelX}},
			&shim.Column{Value: &shim.Column_String_{String_: accelY}},
			&shim.Column{Value: &shim.Column_String_{String_: accelZ}},
			&shim.Column{Value: &shim.Column_String_{String_: gyroX}},
			&shim.Column{Value: &shim.Column_String_{String_: gyroY}},
			&shim.Column{Value: &shim.Column_String_{String_: gyroZ}},
			&shim.Column{Value: &shim.Column_String_{String_: magX}},
			&shim.Column{Value: &shim.Column_String_{String_: magY}},
			&shim.Column{Value: &shim.Column_String_{String_: magZ}},
			&shim.Column{Value: &shim.Column_String_{String_: light}},
			&shim.Column{Value: &shim.Column_String_{String_: time}},
		}})

	if !ok && err == nil {
		return nil, errors.New("Document already exists in IOTTable.")
	}

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("After Row Insertion : ", ok)
	
	//function to get cargolocation based on iothub

	var CargoLocation string
	
	validIOTHub := map[string]bool{"ipad01": true, "ipad02": true, "ipad03": true}

	if !validIOTHub[iothub] {
		myLoggerIOT.Debugf("-------------------------------------------------------------------")
		myLoggerIOT.Debugf("Cargo Location Not Found!")
		return nil, errors.New("Cargo Location Not Found!")	
	} 
	
	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Cargo Location Found!",iothub)
	
	if iothub == "ipad01" {
		CargoLocation = "Ex FWD"
	} else if iothub == "ipad02" {
		CargoLocation = "Ex Ship"
	} else if iothub == "ipad03" {
		CargoLocation = "Shipping"
	}
	
	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Cargo Location Set : ", CargoLocation)
	
	toSend := make([]string, 3)
	toSend[0] = string(ContractNo)
	toSend[1] = string(CargoLocation)
	toSend[2] = string(time)

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Before Update Cargo Location (Contract No) : ", toSend[0])
	myLoggerIOT.Debugf("Before Update Cargo Location (CargoLocation) : ", toSend[1])
	myLoggerIOT.Debugf("Before Update Cargo Location (Time) : ", toSend[2])
	
	clupdate, clErr := t.cl.UpdateCargoLocation(stub, toSend)
	if clErr != nil {
		return nil, clErr
	}

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("After Update Cargo Location (Contract No) : ", clupdate)
	myLoggerIOT.Debugf("Error After Update Cargo Location (Contract No) : ", clErr)
		
	return nil, err
}


/*func (t *IOT) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if function == "GetIOTdata" {
		return t.GetIOTdata(stub, args)
	}
	return nil, errors.New("Received unknown function invocation")
}
*/
func (t *IOT) GetIOTdata(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1.")
	}
	ContractNo := args[0]
	myLoggerIOT.Debugf("Contract number : ", ContractNo)

	// Get the row pertaining to this UID
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "IOT"}}
	columns = append(columns, col1)
	col2 := shim.Column{Value: &shim.Column_String_{String_: ContractNo}}
	columns = append(columns, col2)

	row, err := stub.GetRow("IOTTable", columns)
	if err != nil {
		return nil, fmt.Errorf("Error: Failed retrieving document with ContractNo %s. Error %s", ContractNo, err.Error())
	}

	var iotJSON IOTJSON

	// GetRows returns empty message if key does not exist
	if len(row.Columns) == 0 {

		iotJSON.iothub = ""
		iotJSON.deviceid = ""
		iotJSON.ambientTemp = ""
		iotJSON.objectTemp = ""
		iotJSON.humidity = ""
		iotJSON.pressure = ""
		iotJSON.altitude = ""
		iotJSON.accelX = ""
		iotJSON.accelY = ""
		iotJSON.accelZ = ""
		iotJSON.gyroX = ""
		iotJSON.gyroY = ""
		iotJSON.gyroZ = ""
		iotJSON.magX = ""
		iotJSON.magY = ""
		iotJSON.magZ = ""
		iotJSON.light = ""
		iotJSON.time = ""

	} else {

		iotJSON.iothub = row.Columns[2].GetString_()
		iotJSON.deviceid = row.Columns[3].GetString_()
		iotJSON.ambientTemp = row.Columns[4].GetString_()
		iotJSON.objectTemp = row.Columns[5].GetString_()
		iotJSON.humidity = row.Columns[6].GetString_()
		iotJSON.pressure = row.Columns[7].GetString_()
		iotJSON.altitude = row.Columns[8].GetString_()
		iotJSON.accelX = row.Columns[9].GetString_()
		iotJSON.accelY = row.Columns[10].GetString_()
		iotJSON.accelZ = row.Columns[11].GetString_()
		iotJSON.gyroX = row.Columns[12].GetString_()
		iotJSON.gyroY = row.Columns[13].GetString_()
		iotJSON.gyroZ = row.Columns[14].GetString_()
		iotJSON.magX = row.Columns[15].GetString_()
		iotJSON.magY = row.Columns[16].GetString_()
		iotJSON.magZ = row.Columns[17].GetString_()
		iotJSON.light = row.Columns[18].GetString_()
		iotJSON.time = row.Columns[19].GetString_()
	}

	jsonIOT, err := json.Marshal(iotJSON)
	if err != nil {
		return nil, err
	}
	myLoggerIOT.Debugf("IOT Data : ", jsonIOT)
	return jsonIOT, nil
}

/*func main() {
	err := shim.Start(new(ServicesChaincode))
	if err != nil {
		fmt.Printf("Error starting ServicesChaincode: %s", err)
	}
}
*/
