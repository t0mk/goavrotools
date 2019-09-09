package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/linkedin/goavro"
	"github.com/urfave/cli"
)

const TailUsage = "goavrotools tail <avrofile> <tailcount> <outfile>"
const CountUsage = "goavrotools count <avrofile>"

func GetRecordCount(fn string) (int, error) {
	rf, err := os.Open(fn)
	if err != nil {
		return -1, err
	}
	defer rf.Close()

	oreader, err := goavro.NewOCFReader(rf)
	if err != nil {
		return -1, err
	}
	total := 0
	for oreader.Scan() {
		_, err := oreader.Read()
		if err != nil {
			return -1, err
		}
		total += 1
	}
	return total, nil

}
func Count(c *cli.Context) error {
	avrofile := c.Args().Get(0)
	if len(c.Args()) != 1 {
		return fmt.Errorf("Needs 1 arg, %s", CountUsage)
	}

	total, err := GetRecordCount(avrofile)
	if err != nil {
		return err
	}
	fmt.Println(total)
	return nil
}

func Tail(c *cli.Context) error {
	avrofile := c.Args().Get(0)
	tailcount, err := strconv.Atoi(c.Args().Get(1))
	if err != nil {
		return err
	}
	outfile := c.Args().Get(2)
	if len(c.Args()) != 3 {
		return fmt.Errorf("Needs 3 args, %s", TailUsage)
	}

	total, err := GetRecordCount(avrofile)
	if err != nil {
		return err
	}
	if tailcount >= total {
		log.Println("tail is >= total num of records, use cp")
		return nil
	}
	fmt.Println("Total records in source file:", total)
	tailix := total - tailcount - 1

	rf, err := os.Open(avrofile)
	if err != nil {
		return err
	}
	defer rf.Close()

	oreader, err := goavro.NewOCFReader(rf)
	if err != nil {
		return err
	}

	count := 0
	wf, err := os.OpenFile(outfile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer wf.Close()

	cfg := goavro.OCFConfig{Codec: oreader.Codec(), W: wf}

	w, err := goavro.NewOCFWriter(cfg)
	if err != nil {
		return err
	}

	for oreader.Scan() {
		d, err := oreader.Read()
		if err != nil {
			return err
		}
		if count > tailix {
			err := w.Append([]interface{}{d})
			if err != nil {
				fmt.Println(d, count)
				panic(err)
			}
		}
		count += 1

	}

	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "goavrotools"
	app.Commands = []cli.Command{
		{
			Name:    "tail",
			Aliases: []string{"t"},
			Usage:   TailUsage,
			Action:  Tail,
		},
		{
			Name:    "count",
			Aliases: []string{"c"},
			Usage:   CountUsage,
			Action:  Count,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}
