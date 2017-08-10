package main

import (
	"encoding/json"
	"errors"
	//"fmt"
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

type EVENTJSON struct {
	ContractNo  string `json:"contractNo"`
	Iothub      string `json:"iothub"`
	Deviceid    string `json:"deviceid"`
	Time        string `json:"time"`
	AmbientTemp string `json:"ambientTemp"`
	ObjectTemp  string `json:"objectTemp"`
	Email 	    string `json:"Email"`
}

type IOTJSON struct {
	Iothub      string `json:"iothub"`
	Deviceid    string `json:"deviceid"`
	AmbientTemp string `json:"ambientTemp"`
	ObjectTemp  string `json:"objectTemp"`
	Humidity    string `json:"humidity"`
	Pressure    string `json:"pressure"`
	Altitude    string `json:"altitude"`
	AccelX      string `json:"accelX"`
	AccelY      string `json:"accelY"`
	AccelZ      string `json:"accelZ"`
	GyroX       string `json:"gyroX"`
	GyroY       string `json:"gyroY"`
	GyroZ       string `json:"gyroZ"`
	MagX        string `json:"magX"`
	MagY        string `json:"magY"`
	MagZ        string `json:"magZ"`
	Light       string `json:"light"`
	Time        string `json:"time"`
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
		&shim.ColumnDefinition{Name: "ContractNoLocation", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "ContractNo", Type: shim.ColumnDefinition_STRING, Key: false},
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
	myLoggerIOT.Debugf("GetContractNo : ", string(b1))

	e1, err := t.drr.GetEmailId(stub, []string{deviceid})

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Error Just after GetEmailId", err)

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Just after GetEmailId")

	if e1 == nil {
		myLoggerIOT.Debugf("-------------------------------------------------------------------")
		myLoggerIOT.Debugf("Before e1 = NIL")
		return nil, errors.New("Email ID Not Found")
	}

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("GetEmailId : ", string(e1))

	ContractNo := contractid.ContractNo
	Email := string(e1)
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

	LatestLocation, err := t.cl.GetCargoLocation(stub, []string{ContractNo})

	if LatestLocation == nil {
		myLoggerIOT.Debugf("-------------------------------------------------------------------")
		return nil, errors.New("CargoLocation Not Found!")
	}

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Error Just after GetCargoLocation", err)

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("GetCargoLocation : ", string(LatestLocation))
	
	ContractNoLocation := ContractNo + iothub

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

	if string(CargoLocation) == string(LatestLocation) {
		myLoggerIOT.Debugf("------------------------------------------------------")
		myLoggerIOT.Debugf("Cargo Location Matched ", string(LatestLocation))
	}
	
	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Cargo Location Set : ", CargoLocation)
	
	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("ContractNoLocation : ", ContractNoLocation)

	// Insert a row
	ok, err := stub.InsertRow("IOTTable", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: "IOT"}},
			&shim.Column{Value: &shim.Column_String_{String_: ContractNoLocation}},
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

	var eventJSON EVENTJSON

	eventJSON.ContractNo = ContractNo
	eventJSON.Iothub = iothub
	eventJSON.Deviceid = deviceid
	eventJSON.Time = time
	eventJSON.AmbientTemp = ambientTemp
	eventJSON.ObjectTemp = objectTemp
	eventJSON.Email = Email

	myLoggerIOT.Debugf("eventJSON", eventJSON)
	jsonEvent, err := json.Marshal(eventJSON)

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Error in Marshalling : ",err)

	if err != nil {
		return nil, err
	}
	myLoggerIOT.Debugf("Event Data : ", string(jsonEvent))

	err = stub.SetEvent("IOTSubmitEvent", jsonEvent)
	if err != nil {
		return nil, err
	}

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("After Set Event: ", clupdate)
	myLoggerIOT.Debugf("Error After Set Event: ", clErr)

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

	if len(args) != 2 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2.")
	}
	ContractNo := args[0]
	Location := args[1]
	myLoggerIOT.Debugf("Contract number : ", ContractNo)
	myLoggerIOT.Debugf("Location : ", Location)
	ContractNoLocation := ContractNo + Location

	// Get the row pertaining to this UID
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "IOT"}}
	columns = append(columns, col1)
	col2 := shim.Column{Value: &shim.Column_String_{String_: ContractNoLocation}}
	columns = append(columns, col2)

	row, err := stub.GetRow("IOTTable", columns)
	if err != nil {
		return nil, errors.New("Error: Failed retrieving document!")
	}

	var iotJSON IOTJSON

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Matched Row : ", len(row.Columns))

	// GetRows returns empty message if key does not exist
	if len(row.Columns) == 0 {

		myLoggerIOT.Debugf("-------------------------------------------------------------------")
		myLoggerIOT.Debugf(" Contract No Not Found! ")

		iotJSON.Iothub = ""
		iotJSON.Deviceid = ""
		iotJSON.AmbientTemp = ""
		iotJSON.ObjectTemp = ""
		iotJSON.Humidity = ""
		iotJSON.Pressure = ""
		iotJSON.Altitude = ""
		iotJSON.AccelX = ""
		iotJSON.AccelY = ""
		iotJSON.AccelZ = ""
		iotJSON.GyroX = ""
		iotJSON.GyroY = ""
		iotJSON.GyroZ = ""
		iotJSON.MagX = ""
		iotJSON.MagY = ""
		iotJSON.MagZ = ""
		iotJSON.Light = ""
		iotJSON.Time = ""

	} else {

		myLoggerIOT.Debugf("-------------------------------------------------------------------")
		myLoggerIOT.Debugf("Before Retrieving Data")

		iotJSON.Iothub = row.Columns[3].GetString_()
		iotJSON.Deviceid = row.Columns[4].GetString_()
		iotJSON.AmbientTemp = row.Columns[5].GetString_()
		iotJSON.ObjectTemp = row.Columns[6].GetString_()
		iotJSON.Humidity = row.Columns[7].GetString_()
		iotJSON.Pressure = row.Columns[8].GetString_()
		iotJSON.Altitude = row.Columns[9].GetString_()
		iotJSON.AccelX = row.Columns[10].GetString_()
		iotJSON.AccelY = row.Columns[11].GetString_()
		iotJSON.AccelZ = row.Columns[12].GetString_()
		iotJSON.GyroX = row.Columns[13].GetString_()
		iotJSON.GyroY = row.Columns[14].GetString_()
		iotJSON.GyroZ = row.Columns[15].GetString_()
		iotJSON.MagX = row.Columns[16].GetString_()
		iotJSON.MagY = row.Columns[17].GetString_()
		iotJSON.MagZ = row.Columns[18].GetString_()
		iotJSON.Light = row.Columns[19].GetString_()
		iotJSON.Time = row.Columns[20].GetString_()

		myLoggerIOT.Debugf("-------------------------------------------------------------------")
		myLoggerIOT.Debugf(iotJSON.Iothub)
		myLoggerIOT.Debugf(iotJSON.Deviceid)
		myLoggerIOT.Debugf(iotJSON.AmbientTemp)
		myLoggerIOT.Debugf(iotJSON.ObjectTemp)
		myLoggerIOT.Debugf(iotJSON.Humidity)
		myLoggerIOT.Debugf(iotJSON.Pressure)
		myLoggerIOT.Debugf(iotJSON.Altitude)
		myLoggerIOT.Debugf(iotJSON.AccelX)
		myLoggerIOT.Debugf(iotJSON.AccelY)
		myLoggerIOT.Debugf(iotJSON.AccelZ)
		myLoggerIOT.Debugf(iotJSON.GyroX)
		myLoggerIOT.Debugf(iotJSON.GyroY)
		myLoggerIOT.Debugf(iotJSON.GyroZ)
		myLoggerIOT.Debugf(iotJSON.MagX)
		myLoggerIOT.Debugf(iotJSON.MagY)
		myLoggerIOT.Debugf(iotJSON.MagZ)
		myLoggerIOT.Debugf(iotJSON.Light)
		myLoggerIOT.Debugf(iotJSON.Time)
	}
	myLoggerIOT.Debugf("iotJSON", iotJSON)
	jsonIOT, err := json.Marshal(iotJSON)

	myLoggerIOT.Debugf("-------------------------------------------------------------------")
	myLoggerIOT.Debugf("Error in Marshalling : ",err)

	if err != nil {
		return nil, err
	}
	myLoggerIOT.Debugf("IOT Data : ", string(jsonIOT))
	return jsonIOT, nil
}

/*func main() {
	err := shim.Start(new(ServicesChaincode))
	if err != nil {
		fmt.Printf("Error starting ServicesChaincode: %s", err)
	}
}
*/
