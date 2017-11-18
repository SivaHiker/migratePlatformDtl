package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
	"fmt"
	"strconv"
	"strings"
	"os"
	"bufio"
	"encoding/csv"
	"bytes"
	"io"
)

func main() {

	var count2 int64
	var recordsCount int64
	file, err := os.Open("/home/siva/LatestAppOpenUsers_20170512_to_20171107.txt")
	defer file.Close()

	if err != nil {
		println(err)
	}

	dbConn := getDBConnection()
	dbConn.SetMaxOpenConns(10)

	defer dbConn.Close()
	err = dbConn.Ping()
	if err != nil {
		fmt.Println(err.Error())
	}
	// Start reading from the file with a reader.
	reader := bufio.NewReader(file)

	//outputfile1, err := os.Create("text2.txt")
	//if(err!=nil){
	//	fmt.Println("Not able to create a file")
	//}
	//defer outputfile1.Close()

	csvfile1, err := os.Create("resultPlatformDtl.csv")
	if(err!=nil){
		fmt.Println("Not able to create a csv file")
	}

	writer1 := csv.NewWriter(csvfile1)
	defer writer1.Flush()
	defer csvfile1.Close()


	limiter := time.Tick(time.Nanosecond * 333333333)

	var linecount int
	for {
		linecount =0
		var userdetails platformUserDetails
		lines := make([]string, 2000)
		query := ""
		for linecount<1000 {
			var buffer bytes.Buffer
			var line []byte
			line, _, err = reader.ReadLine()
			buffer.Write(line)
			println(buffer.String())
			// If we're just at the EOF, break
			if err != nil {
				if query=="" {
					fmt.Println("Final Number of records exported from the DB",recordsCount)
					os.Exit(1)
				} else {
					break
				}
			} else {
				uidString := string(line[:])
				uid :=uidString[0:16]
				lines = append(lines,uid)
				if linecount == 0 {
					query = query + "\"" + strings.TrimSpace(uid) + "\""
				} else {
					query = query + ",\"" + strings.TrimSpace(uid) + "\""
				}
				linecount++
			}
		}

		<-limiter
		rows2,err := dbConn.Query("select * from platform_user_details where  hike_uid in ("+query+")")
		if(err!=nil){
			fmt.Println(err)
		}


		for rows2.Next() {
			err := rows2.Scan(&userdetails.ID,&userdetails.HikeUID, &userdetails.Msisdn, &userdetails.Name,
				&userdetails.Gender,&userdetails.Circle, &userdetails.CreateTime, &userdetails.UpdateTime)
			fmt.Println(err)

			userDetailCreateTime := strings.Split(userdetails.CreateTime.String(),"+")
			userDtlCrTime := userDetailCreateTime[0]


			userDetailUpdateTime := strings.Split(userdetails.UpdateTime.String(),"+")
			userDtlUpTime := userDetailUpdateTime[0]


			msisdnReqd2 := userdetails.Msisdn
			if strings.HasPrefix(msisdnReqd2,"+9") {
				msisdnReqd2=strings.Replace(msisdnReqd2,"+9","1",1)
			} else if strings.HasPrefix(msisdnReqd2,"+8") {
				msisdnReqd2=strings.Replace(msisdnReqd2,"+8","2",1)
			} else if strings.HasPrefix(msisdnReqd2,"+7") {
				msisdnReqd2=strings.Replace(msisdnReqd2,"+7","3",1)
			} else {
				continue
			}

			count2++
			recordsCount++
			records2 := [][]string{
				{ToIntegerVal(count2),userdetails.HikeUID,"+"+msisdnReqd2,ToString(userdetails.Name),
					ToString(userdetails.Gender),ToString(userdetails.Circle), strings.TrimSpace(userDtlCrTime),strings.TrimSpace(userDtlUpTime)},
			}

			for _, value := range records2 {
				err := writer1.Write(value)
				if(err!=nil){
					fmt.Println(err.Error())
					fmt.Println("Not able to write the records into csv file")
				}
			}

			//outputfile1.WriteString(ToIntegerVal(count2)+"::"+userdetails.HikeUID+"::"+"+"+msisdnReqd2+"::"+ToString(
			//	userdetails.Name)+"::"+ ToString(userdetails.Gender)+"::"+ToString(userdetails.Circle)+"::"+
			//	"::"+strings.TrimSpace(userDtlCrTime)+"::"+strings.TrimSpace(userDtlUpTime)+"\n")
		}

		rows2.Close()
		fmt.Println("Number of records exported from the DB",recordsCount)
	}
	fmt.Println("Final Number of records exported from the DB",recordsCount)

	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}

}


type platformUserDetails struct {
	Circle     sql.NullString `json:"circle"`
	CreateTime time.Time `json:"create_time"`
	Gender     sql.NullString `json:"gender"`
	HikeUID    string `json:"hike_uid"`
	ID         int64    `json:"id"`
	Msisdn     string `json:"msisdn"`
	Name       sql.NullString `json:"name"`
	UpdateTime time.Time `json:"update_time"`
}


func getDBConnection() *sql.DB{

	db, err := sql.Open("mysql", "platform:p1@tf0rmD1st@tcp(10.9.33.14:3306)/usersdb?parseTime=true")
	if(err!=nil){
		fmt.Println(err)
	}
	return db
}

func ToNullString(s string) sql.NullString {
	return sql.NullString{String : s, Valid : s != ""}
}

func ToIntegerVal(i int64) string {
	var valueInt string
	valueInt = strconv.FormatInt(int64(i), 10)
	return valueInt
}

func ToStringFromInt(i int) string {
	var valueInt string
	valueInt = strconv.Itoa(i)
	return valueInt
}

func ToString(s sql.NullString) string {
	var valInString string
	if(s.Valid) {
		valInString = s.String
		fmt.Println(valInString)
	} else {
		valInString = "NULL"
		fmt.Println(valInString)
	}
	return valInString
}