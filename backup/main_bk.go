package main

import (
	_ "unicode"
	"os"
	"encoding/csv"
	"strconv"
	"time"
	"fmt"
	"sync"
)

type Product struct {
	PartNumber string
	UnitCost float64
	UnitPrice float64
}

type Order struct {
	CustomerNumber int
	PartNumber string
	Quantity int

	UnitCost float64
	UnitPrice float64
}


func main(){
	start := time.Now()
	//ประกาศ channel
	extractChannel := make(chan *Order)
	transformChannel := make(chan *Order)
	doneChannel := make(chan bool)

	//orders := extract()
	go extract(extractChannel)
	//orders = transform(orders)
	fmt.Print("extract finished->")
	fmt.Println(time.Since(start))
	go transform(extractChannel,transformChannel)
	fmt.Print("transform finished->")
	fmt.Println(time.Since(start))

	go load(transformChannel, doneChannel)
	fmt.Print("Load finished->")
	<- doneChannel
	fmt.Println(time.Since(start))

}


func extract(ch chan *Order)  {
	//result :=  []*Order{}

	f,_ := os.Open("./orders.txt")
	defer f.Close()
	r:= csv.NewReader(f)

	for record, err := r.Read();err == nil; record, err = r.Read(){
		//fmt.Println("extract ",record)
		order := new(Order)
		order.CustomerNumber, _ = strconv.Atoi(record[0])
		order.PartNumber = record[1]
		order.Quantity ,_ = strconv.Atoi(record[2])
		//result = append(result, order)
		ch <- order

	}
	close(ch)

	//return result
}

func transform(extractChannel, transformChannel chan *Order) {
	f, _ := os.Open("./productList.txt")
	defer f.Close()
	r := csv.NewReader(f)

	// อ่านข้อมูลสินค้าจาก txt และ สร้าง map เพื่อใช้สำหรับการค้นหา โดยมี index = PartNumber
	records, _ := r.ReadAll()
	productList := make(map[string]*Product)
	for _, record := range records {
		product :=  new(Product)
		product.PartNumber = record[0]
		product.UnitCost, _ = strconv.ParseFloat(record[1], 64)
		product.UnitPrice, _  = strconv.ParseFloat(record[2], 64)
		productList[product.PartNumber] = product
	}

	w := sync.WaitGroup{}
	//numMessages := 0
	//for idx, _ := range orders {
	for o := range extractChannel{
		//fmt.Println("idx : ",idx)
		//numMessages++
		w.Add(1)
		go func(o *Order){
			time.Sleep(1 * time.Millisecond)
			//o := orders[idx]
			o.UnitCost = productList[o.PartNumber].UnitCost
			o.UnitPrice = productList[o.PartNumber].UnitPrice
			transformChannel <- o
			//numMessages--
			w.Done()
		}(o)


		//for ;numMessages > 0;{
		//	time.Sleep(1* time.Millisecond)
		//}
		w.Wait()
	}
	// ปิดประตู ทางเข้า chanel transform
	close(transformChannel)


	//return orders
	// end of transform
}


//func load(orders []*Order) {
func load(transformChannel chan *Order,doneChannel chan bool){
	f, _ := os.Create("./dest.txt")
	defer f.Close()

	fmt.Fprintf(f, "%20s%15s%12s%12s%15s%15%s",
		"Part Number", "Quantity", "Unit Cost",
		"Unit Price", "Total Cost", "Total Price")

	//for _, o := range orders {
	w:=sync.WaitGroup{}
	//numMessages := 0
	//numRoutine :=0
	for o:= range transformChannel{
		//numMessages++
		w.Add(1)
		go func(o *Order){
			//numRoutine++
			time.Sleep(1 * time.Millisecond)
			fmt.Fprintf(f, "%20s %15d %12.2f %12.2f %15.2f %15.2f\n",
				o.PartNumber, o.Quantity, o.UnitCost, o.UnitPrice ,
				o.UnitCost * float64(o.Quantity),
				o.UnitPrice * float64(o.Quantity))
			w.Done()
			//numMessages--
			//fmt.Println("Load : number of Routine  : ",numRoutine)
		}(o)

	}
	// ส่งข้อมูล ทำงานสำเร็จ เข้า channel

	//for ;numMessages > 0;{
	//	time.Sleep(1* time.Millisecond)
	//}
	w.Wait()
	doneChannel <- true
}
