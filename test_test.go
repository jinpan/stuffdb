package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/jinpan/stuffdb/datatypes"
	"github.com/jinpan/stuffdb/schema"
	"github.com/jinpan/stuffdb/table"
)

const (
	N_COLS          = 196
	TEST_TABLE_NAME = "test_census"
)

var (
	TEST_PATH = path.Join(
		"/var",
		"stuffdb",
		TEST_TABLE_NAME,
	)
)

func cleanup(t *testing.T) {
	remove_err := os.RemoveAll(TEST_PATH)
	if remove_err != nil {
		t.Errorf(remove_err.Error())
	}
}

func makeSchema() *schema.Schema {
	names := []string{
		"serialno", "sporder", "puma00", "puma10", "st", "adjinc", "pwgtp", "agep",
		"cit", "dear", "deye", "hins1", "hins2", "hins3", "hins4", "hins5", "hins6",
		"hins7", "mar", "relp", "sex", "anc", "anc1p05", "anc1p12", "anc2p05",
		"anc2p12", "dis", "hicov", "hisp", "nativity", "oc", "pobp05", "pobp12",
		"privcov", "pubcov", "qtrbir", "rac1p", "rac2p05", "rac2p12", "rac3p05",
		"rac3p12", "racaian", "racasn", "racblk", "racnhpi", "racnum", "racsor",
		"racwht", "rc", "waob", "fagep", "fancp", "fcitp", "fcitwp", "fcowp",
		"fddrsp", "fdearp", "fdeyep", "fdoutp", "fdphyp", "fdratp", "fdratxp",
		"fdremp", "fengp", "fesrp", "fferp", "ffodp", "fgclp", "fgcmp", "fgcrp",
		"fhins1p", "fhins2p", "fhins3p", "fhins4p", "fhins5p", "fhins6p", "fhins7p",
		"fhisp", "findp", "fintp", "fjwdp", "fjwmnp", "fjwrip", "fjwtrp", "flanp",
		"flanxp", "fmarhdp", "fmarhmp", "fmarhtp", "fmarhwp", "fmarhyp", "fmarp",
		"fmigp", "fmigsp", "fmilpp", "fmilsp", "foccp", "foip", "fpap", "fpobp",
		"fpowsp", "fracp", "frelp", "fretp", "fschgp", "fschlp", "fschp", "fsemp",
		"fsexp", "fssip", "fssp", "fwagp", "fwkhp", "fwklp", "fwkwp", "fyoep",
		"pwgtp1", "pwgtp2", "pwgtp3", "pwgtp4", "pwgtp5", "pwgtp6", "pwgtp7",
		"pwgtp8", "pwgtp9", "pwgtp10", "pwgtp11", "pwgtp12", "pwgtp13", "pwgtp14",
		"pwgtp15", "pwgtp16", "pwgtp17", "pwgtp18", "pwgtp19", "pwgtp20", "pwgtp21",
		"pwgtp22", "pwgtp23", "pwgtp24", "pwgtp25", "pwgtp26", "pwgtp27", "pwgtp28",
		"pwgtp29", "pwgtp30", "pwgtp31", "pwgtp32", "pwgtp33", "pwgtp34", "pwgtp35",
		"pwgtp36", "pwgtp37", "pwgtp38", "pwgtp39", "pwgtp40", "pwgtp41", "pwgtp42",
		"pwgtp43", "pwgtp44", "pwgtp45", "pwgtp46", "pwgtp47", "pwgtp48", "pwgtp49",
		"pwgtp50", "pwgtp51", "pwgtp52", "pwgtp53", "pwgtp54", "pwgtp55", "pwgtp56",
		"pwgtp57", "pwgtp58", "pwgtp59", "pwgtp60", "pwgtp61", "pwgtp62", "pwgtp63",
		"pwgtp64", "pwgtp65", "pwgtp66", "pwgtp67", "pwgtp68", "pwgtp69", "pwgtp70",
		"pwgtp71", "pwgtp72", "pwgtp73", "pwgtp74", "pwgtp75", "pwgtp76", "pwgtp77",
		"pwgtp78", "pwgtp79", "pwgtp80",
	}
	types := make([]datatypes.DatumType, len(names))
	for i := 0; i < len(types); i++ {
		types[i] = datatypes.INT64_TYPE
	}

	schema, err := schema.NewSchema(names, types)
	if err != nil {
		panic(err.Error())
	}
	return schema
}

func TestStress(t *testing.T) {
	defer cleanup(t)
	s := makeSchema()
	table := table.NewTable(TEST_TABLE_NAME, s)
	if table == nil {
		t.Errorf("Unable to create new table")
	}

	f, err := os.Open("/home/jinpan/downloads/output.csv")
	if err != nil {
		t.Errorf(err.Error())
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	count := 0

	rows := make(chan []interface{})
	go func() {
		for scanner.Scan() {
			if count%1000 == 0 {
				fmt.Println(count)
			}

			line := scanner.Text()
			parts := strings.Split(line, ",")
			if len(parts) != N_COLS {
				t.Errorf("Expected %d cols, got %d", N_COLS, len(parts))
			}
			row := make([]interface{}, N_COLS)
			for i, part := range parts {
				num, err := strconv.ParseInt(part, 10, 64)
				if err != nil {
					t.Errorf(err.Error())
				}
				row[i] = num
			}
			rows <- row
			count++
		}
		close(rows)
	}()
	table.BulkInsert(rows, 1821870)

	// b.ResetTimer()
}
