package flash

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/TerrexTech/go-mongoutils/mongo"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/pkg/errors"
)

type ConfigSchema struct {
	Flash     *Flash
	Metric    *Metric
	Inventory *Inventory
}

type DBIConfig struct {
	Hosts               []string
	Username            string
	Password            string
	TimeoutMilliseconds uint32
	Database            string
	Collection          string
}

type DBI interface {
	Collection() *mongo.Collection
	UpdateFlashSale(fsale Flash) (*mgo.UpdateResult, error)
}

type DB struct {
	collection *mongo.Collection
}

func GenerateDB(dbConfig DBIConfig, schema interface{}) (*DB, error) {
	config := mongo.ClientConfig{
		Hosts:               dbConfig.Hosts,
		Username:            dbConfig.Username,
		Password:            dbConfig.Password,
		TimeoutMilliseconds: dbConfig.TimeoutMilliseconds,
	}

	client, err := mongo.NewClient(config)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}

	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: 5000,
	}

	// indexConfigs := []mongo.IndexConfig{
	// 	mongo.IndexConfig{
	// 		ColumnConfig: []mongo.IndexColumnConfig{
	// 			mongo.IndexColumnConfig{
	// 				Name: "item_id",
	// 			},
	// 		},
	// 		IsUnique: true,
	// 		Name:     "item_id_index",
	// 	},
	// 	mongo.IndexConfig{
	// 		ColumnConfig: []mongo.IndexColumnConfig{
	// 			mongo.IndexColumnConfig{
	// 				Name:        "timestamp",
	// 				IsDescOrder: true,
	// 			},
	// 		},
	// 		IsUnique: true,
	// 		Name:     "timestamp_index",
	// 	},
	// }

	// ====> Create New Collection
	collConfig := &mongo.Collection{
		Connection:   conn,
		Database:     dbConfig.Database,
		Name:         dbConfig.Collection,
		SchemaStruct: schema,
		// Indexes:      indexConfigs,
	}
	c, err := mongo.EnsureCollection(collConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}
	return &DB{
		collection: c,
	}, nil
}

func (d *DB) Collection() *mongo.Collection {
	return d.collection
}

func (db *DB) UpdateFlashSale(fsale Flash) (*mgo.UpdateResult, error) {
	filter := &Flash{
		FlashID: fsale.FlashID,
	}

	if fsale.ItemID.String() == "00000000-0000-0000-0000-000000000000" {
		log.Println("ItemID is empty")
		return nil, errors.New("ItemID not found")
	}

	if fsale.UPC == 0 {
		log.Println("UPC is empty")
		return nil, errors.New("UPC not found")
	}

	if fsale.SKU == 0 {
		log.Println("SKU is empty")
		return nil, errors.New("SKU not found")
	}

	if fsale.Name == "" {
		log.Println("Name is empty")
		return nil, errors.New("Name not found")
	}

	if fsale.Origin == "" {
		log.Println("Origin is empty")
		return nil, errors.New("Origin not found")
	}

	if fsale.DeviceID.String() == "00000000-0000-0000-0000-000000000000" {
		log.Println("DeviceID is empty")
		return nil, errors.New("DeviceID not found")
	}

	if fsale.Price == 0 {
		log.Println("Price is empty")
		return nil, errors.New("Price not found")
	}

	if fsale.SalePrice < 0 || fsale.SalePrice > math.MaxInt64 {
		log.Println("Sale Price error. Number is either less than 0 or greater than allowed max value")
		return nil, errors.New("Sale Price not found")
	}

	if fsale.Ethylene == 0 {
		log.Println("Ethylene value is empty")
		return nil, errors.New("Ethylene not found")
	}

	//Adding the timestamp
	nowTime := time.Now().Unix()
	fsale.Timestamp = nowTime

	update := &map[string]interface{}{
		"flash_id":   fsale.FlashID,
		"item_id":    fsale.ItemID,
		"upc":        fsale.UPC,
		"sku":        fsale.SKU,
		"name":       fsale.Name,
		"origin":     fsale.Origin,
		"device_id":  fsale.DeviceID.String(),
		"price":      fsale.Price,
		"sale_price": fsale.SalePrice,
		"ethylene":   fsale.Ethylene,
	}

	updateResult, err := db.collection.UpdateMany(filter, update)
	if err != nil {
		err = errors.Wrap(err, "Unable to update event")
		return nil, err
	}
	fmt.Println(updateResult.ModifiedCount)

	return updateResult, nil

}
