package main

import (
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	fmt "fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

type CatalogoEmissor struct {
	Api             string
	Versao          string
	CnpjInstituicao string
	NomeInstituicao string
	Recurso         string
	Argumento       string
	Situacao        string
	URLDados        string
	URLConsulta     string
}

type Catalogo struct {
	Context string `json:"@odata.context"`
	Value   []CatalogoEmissor
}

type EmissorTaxas struct {
	TaxaTipoGasto          string
	TaxaData               string
	TaxaConversao          float64
	TaxaDivulgacaoDataHora string
}

type Emissor struct {
	EmissorCnpj string
	EmissorNome string
	Historico   []EmissorTaxas
}

type CSVLine struct {
	EmissorCnpj string
	EmissorNome string
	TaxaTipoGasto          string
	TaxaData               string
	TaxaConversao          float64
	TaxaDivulgacaoDataHora string
}

func main() {
	fmt.Printf("Hello, world\n")

	catalogo, err := getCatalogo("ultimo")

	var taxas []Emissor

	if err != nil {
		return
	}

	for _, emissorCatalogo := range catalogo.Value {
		if t, err := getEmissor(emissorCatalogo); err == nil {
			taxas = append(taxas, t)
		} else {
			fmt.Printf("%s\n", err)
		}
	}
	println("Salvando dados no CSV")
	salvaCsv(taxas)
}

func salvaCsv(taxas []Emissor) {
	file, err := os.Create("result.csv")
	checkError("Não foi possivel criar o arquivo", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	err = writer.Write([]string{"nome_emissor", "cnpj_emissor", "dt_movimento", "tipo_gasto", "vl_taxa", "data_hora_divulgacao",})
	checkError("Não foi possivel escrever o cabeçalho", err)

	for _, emissor := range taxas {
		for _, historico := range emissor.Historico {
			csvLine := []string{
				emissor.EmissorNome,
				emissor.EmissorCnpj,
				historico.TaxaData,
				historico.TaxaTipoGasto,
				strconv.FormatFloat(historico.TaxaConversao, 'f', 6, 64),
				historico.TaxaDivulgacaoDataHora,
			}
			err := writer.Write(csvLine)
			checkError("Error writing line", err)
		}
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
func getEmissor(catalogoEmissor CatalogoEmissor) (Emissor, error) {

	var emissor Emissor
	var historico []EmissorTaxas

	/** Ignora erro de Certificado **/
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	response, err := client.Get(catalogoEmissor.URLDados)
	if err != nil {
		fmt.Printf("Houve um erro ao consultar o catalogoEmissor: %s", err)
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Printf("Erro ao ler o corpo da resposta %s", err)
		return emissor, err
	}

	// API tem muitas inconsistencias para usar JSON Estruturado.
	var result map[string]interface{}
	json.Unmarshal(responseBody, &result)

	historicoRaw := result["historicoTaxas"]

	// Verifica o padrão retornado
	switch historicoRaw.(type) {
	case []interface{}:
		historico = convertArrayEmissorTaxa(historicoRaw.([]interface{}))
	case map[string]interface{}:
		taxa := convertEmissorTaxa(historicoRaw.(map[string]interface{}))
		historico = append(historico, taxa)
	default:
	}

	if cnpj, ok := result["emissorCnpj"].(string); ok {
		emissor.EmissorCnpj = cnpj
	}
	if nome, ok := result["emissorNome"].(string); ok {
		emissor.EmissorNome = nome
	}
	if emissor.EmissorNome == "" {
		return emissor, errors.New(fmt.Sprintf("Emissor %s não retornou resultados", catalogoEmissor.NomeInstituicao))
	}

	emissor.Historico = historico

	return emissor, nil
}

func convertArrayEmissorTaxa(emissorTaxaArray []interface{}) []EmissorTaxas {
	var taxas []EmissorTaxas
	for _, item := range emissorTaxaArray {
		if taxa, ok := item.(map[string]interface{}); ok {
			taxas = append(taxas, convertEmissorTaxa(taxa))
		}
	}
	return taxas
}

func convertEmissorTaxa(emissorTaxa map[string]interface{}) EmissorTaxas {
	var emissor EmissorTaxas

	if taxaTipoGasto, ok := emissorTaxa["taxaTipoGasto"].(string); ok {
		emissor.TaxaTipoGasto = taxaTipoGasto
	}
	if taxaData, ok := emissorTaxa["taxaData"].(string); ok {
		emissor.TaxaData = taxaData
	}
	if taxaDivulgacaoDataHora, ok := emissorTaxa["taxaDivulgacaoDataHora"].(string); ok {
		emissor.TaxaDivulgacaoDataHora = taxaDivulgacaoDataHora
	}
	if taxaConversao, ok := emissorTaxa["taxaConversao"].(string); ok {
		if t, err := strconv.ParseFloat(taxaConversao, 64); err == nil {
			emissor.TaxaConversao = t
		}
	} else if taxaConversao, ok := emissorTaxa["taxaConversao"].(float64); ok {
		emissor.TaxaConversao = taxaConversao
	}

	return emissor
}

func getCatalogo(tipo string) (Catalogo, error) {
	url := getCatalogoUrl(tipo)
	var catalogo Catalogo

	fmt.Println(url)

	response, err := http.Get(url)

	if err != nil {
		fmt.Printf("Erro ao fazer request para o servico do Bacen: %s\n", err)
		return catalogo, err
	}

	data, err := ioutil.ReadAll(response.Body)

	if err != nil {
		fmt.Printf("Erro ao ler o catalogo: %s\n", err)
		return catalogo, err
	}

	err = json.Unmarshal(data, &catalogo)

	if err != nil {
		fmt.Printf("Erro ao converter JSON. err = %s", err)
		return catalogo, err
	}

	//fmt.Printf("catalogo: %+v\n", catalogo)
	return catalogo, nil
}

func getCatalogoUrl(tipo string) string {
	var recurso string
	if tipo == "ultimo" {
		recurso = "/itens/ultimo"
	} else {
		recurso = "/itens"
	}

	filtros := "$filter="
	filtrosContent := url.PathEscape(fmt.Sprintf("Api eq 'taxas_cartoes' and Recurso eq '%s' and Situacao eq 'Produção'", recurso))
	filtros = fmt.Sprintf("%s%s", filtros, filtrosContent)

	quantidade := "$top=10000"

	return fmt.Sprintf("https://olinda.bcb.gov.br/olinda/servico/DASFN/versao/v1/odata/Recursos?%s&%s&$format=json", filtros, quantidade)
}
