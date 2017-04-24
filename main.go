package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/garyburd/redigo/redis"
)

var pool = &redis.Pool{
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	},
}

func main() {
	setupInitial := func() {
		db := pool.Get()
		defer db.Close()
		db.Send("FLUSHDB")
		db.Send("SET", "acc:Cash", -20000)
		db.Send("SET", "acc:Acc1", 10000)
		db.Send("SET", "acc:Acc2", 10000)
		db.Flush()
	}

	// cleanup := func() {
	// 	db := pool.Get()
	// 	defer db.Close()
	// 	log.Println("cleanup")
	// 	db.Do("FLUSHDB")
	// }

	run := func(actions []func() error) {
		wg := &sync.WaitGroup{}
		wg.Add(len(actions))
		for i, action := range actions {
			i, action := i, action
			go func() {
				defer wg.Done()
				err := action()
				if err != nil {
					log.Printf("Execute action %d error; %v", i, err)
					return
				}
				log.Printf("Execute action %d completed", i)
			}()
		}
		wg.Wait()
	}

	{
		log.Println("Start")
		setupInitial()
		actions := []func() error{
			func() error { return deposit("Acc1", 1000) },
			func() error { return deposit("Acc1", 2000) },
			func() error { return deposit("Acc2", 5000) },
			func() error { return withdraw("Acc1", 1000) },
			func() error { return withdraw("Acc2", 1000) },
			func() error { return transfer("Acc1", "Acc2", 2000) },
			func() error { return transfer("Acc2", "Acc1", 4000) },
			func() error { return deposit("Acc1", 10000) },
			func() error { return withdraw("Acc1", 9000) },
		}
		run(actions)
		validate()
		log.Println("Done")
	}
}

type transaction struct {
	ID        int
	Amount    int64
	AccountID string
	Type      string
}

func encode(v interface{}) []byte {
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(v)
	return buf.Bytes()
}

func decode(b []byte, v interface{}) {
	gob.NewDecoder(bytes.NewReader(b)).Decode(v)
}

var errAbout = errors.New("abort")

func retry(f func() error, count int) error {
	var err error
	for i := 0; i < count; i++ {
		err = f()
		if err != errAbout {
			return err
		}
	}
	return err
}

func deposit(accID string, amount int64) error {
	if amount < 0 {
		return fmt.Errorf("invalid amount")
	}

	keyCash := "acc:Cash"
	keyAcc := "acc:" + accID

	trans := []*transaction{
		{Type: "deposit", AccountID: accID, Amount: amount},
		{Type: "withdraw", AccountID: "Cash", Amount: -amount},
	}

	err := retry(func() error {
		db := pool.Get()
		defer db.Close()
		db.Do("WATCH", "id:tran")
		tranID, _ := redis.Int(db.Do("GET", "id:tran"))
		trans[0].ID = tranID + 1
		trans[1].ID = tranID + 2

		db.Send("MULTI")
		db.Send("SET", "id:tran", tranID+2)
		db.Send("DECRBY", keyCash, amount)
		db.Send("INCRBY", keyAcc, amount)
		for _, tran := range trans {
			db.Send("SET", "tran:"+strconv.Itoa(tran.ID), encode(tran))
		}
		ok, err := db.Do("EXEC")
		if err != nil {
			return err
		}
		if ok == nil {
			return errAbout
		}
		return nil
	}, 10)
	if err != nil {
		return err
	}
	return nil
}

func withdraw(accID string, amount int64) error {
	if amount < 0 {
		return fmt.Errorf("invalid amount")
	}

	keyCash := "acc:Cash"
	keyAcc := "acc:" + accID

	trans := []*transaction{
		{Type: "withdraw", AccountID: accID, Amount: -amount},
		{Type: "deposit", AccountID: "Cash", Amount: amount},
	}

	err := retry(func() error {
		db := pool.Get()
		defer db.Close()

		db.Do("WATCH", "id:tran", keyAcc)

		balance, _ := redis.Int64(db.Do("GET", keyAcc))
		if balance < amount {
			return fmt.Errorf("balance not enough for withdraw")
		}

		tranID, _ := redis.Int(db.Do("GET", "id:tran"))
		trans[0].ID = tranID + 1
		trans[1].ID = tranID + 2

		db.Send("MULTI")
		db.Send("SET", "id:tran", tranID+2)
		db.Send("INCRBY", keyCash, amount)
		db.Send("DECRBY", keyAcc, amount)
		for _, tran := range trans {
			db.Send("SET", "tran:"+strconv.Itoa(tran.ID), encode(tran))
		}
		ok, err := db.Do("EXEC")
		if err != nil {
			return err
		}
		if ok == nil {
			return errAbout
		}
		return nil
	}, 10)
	if err != nil {
		return err
	}
	return nil
}

func transfer(from, to string, amount int64) error {
	if amount < 0 {
		return fmt.Errorf("invalid amount")
	}

	keyFrom := "acc:" + from
	keyTo := "acc:" + to

	trans := []*transaction{
		{Type: "withdraw", AccountID: from, Amount: -amount},
		{Type: "deposit", AccountID: to, Amount: amount},
	}

	err := retry(func() error {
		db := pool.Get()
		defer db.Close()

		db.Do("WATCH", "id:tran", keyFrom, keyTo)

		balance, _ := redis.Int64(db.Do("GET", keyFrom))
		if balance < amount {
			return fmt.Errorf("balance not enough for transfer")
		}

		tranID, _ := redis.Int(db.Do("GET", "id:tran"))
		trans[0].ID = tranID + 1
		trans[1].ID = tranID + 2

		db.Send("MULTI")
		db.Send("SET", "id:tran", tranID+2)
		db.Send("DECRBY", keyFrom, amount)
		db.Send("INCRBY", keyTo, amount)
		for _, tran := range trans {
			db.Send("SET", "tran:"+strconv.Itoa(tran.ID), encode(tran))
		}
		ok, err := db.Do("EXEC")
		if err != nil {
			return err
		}
		if ok == nil {
			return errAbout
		}
		return nil
	}, 10)
	if err != nil {
		return err
	}
	return nil
}

func validate() {
	db := pool.Get()
	defer db.Close()

	{
		var s int64
		p, _ := redis.Int64(db.Do("GET", "acc:Acc1"))
		s += p
		p, _ = redis.Int64(db.Do("GET", "acc:Acc2"))
		s += p
		p, _ = redis.Int64(db.Do("GET", "acc:Cash"))
		s += p

		if s != 0 {
			log.Println("sum all accounts not equals to 0")
		}
	}

	{
		var s int64
		cnt, _ := redis.Int(db.Do("GET", "id:tran"))
		for i := 1; i <= cnt; i++ {
			var t transaction
			bs, _ := redis.Bytes(db.Do("GET", "tran:"+strconv.Itoa(i)))
			decode(bs, &t)
			s += t.Amount
		}
		if s != 0 {
			log.Println("sum all transactions not equals to 0")
		}
	}
}
