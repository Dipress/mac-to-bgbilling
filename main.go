package main

import (
	"bufio"
	"database/sql"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Client struct {
	Login string
	Mac   string
	Cid   int
}

var db *sql.DB

var query string = `SELECT c.id FROM contract as c, inet_serv_14 as i WHERE c.id = i.contractId AND i.login =  ?`

func main() {
	var err error

	//Вместо filename указываем имя своего файла с данными по PPoE сессиям на NAS сервере
	list := GetDataOnFile("filename")

	//Тут надо указать свои данные для доступа к БД BGBilling
	db, err = sql.Open("mysql", "username:password@tcp(127.0.0.0:3306)/database")

	if err != nil {
		log.Fatalf("Не удалось соединиться: %v", err)
	}

	defer db.Close()

	for _, cli := range list {
		row := db.QueryRow(query, cli.Login)

		err = row.Scan(&cli.Cid)

		if err != nil {
			log.Println(err)
		}

		if cli.MacExists() {
			cli.Update()

		} else {

			cli.Create()
		}

	}

}

//Данная функция считывает данные из файла и сохраняет данные в срезе
func GetDataOnFile(filename string) []Client {
	var result []Client
	file, err := os.Open(filename)

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		result = append(result, Client{strings.Replace(words[2], "name=", "", -1), strings.Replace(words[4], "caller-id=", "", -1), 0})

	}
	return result
}

//Функция проверяет наличие mac-адреса в конкретном договоре
func (c Client) MacExists() bool {
	var count int

	err := db.QueryRow("SELECT COUNT(*) FROM contract_parameter_type_1 WHERE pid=71 AND cid=?", c.Cid).Scan(&count)

	if err != nil {
		return false
	}

	return count > 0
}

//Функция для обновления mac-адреса
func (c Client) Update() error {
	_, err := db.Exec(`UPDATE contract_parameter_type_1 SET val=? WHERE pid=71 AND cid=?`, c.Mac, c.Cid)
	return err
}

//Функция для добавления mac-адреса
func (c Client) Create() error {
	_, err := db.Exec(`INSERT INTO contract_parameter_type_1 (cid, pid, val) VALUES (?, ?, ?)`, c.Cid, 71, c.Mac)
	return err
}
