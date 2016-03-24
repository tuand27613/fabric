/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package obcpbft

import (
	"fmt"
	"strings"

	"github.com/openblockchain/obc-peer/openchain/consensus"

	"github.com/spf13/viper"
)

const configPrefix = "OPENCHAIN_OBCPBFT"

var pluginInstance consensus.Consenter // singleton service
var config *viper.Viper

func init() {
	config = loadConfig()
}

// GetPlugin returns the handle to the Consenter singleton
func GetPlugin(c consensus.Stack) consensus.Consenter {
	if pluginInstance == nil {
		pluginInstance = New(c)
	}
	return pluginInstance
}

// New creates a new Obc* instance that provides the Consenter interface.
// Internally, it uses an opaque pbft-core instance.
func New(stack consensus.Stack) consensus.Consenter {
	// set the whitelist cap in the peer package
	cap := config.GetInt("general.N")
	stack.SetWhitelistCap(cap)

	//instantiate
	mode := strings.ToLower(config.GetString("general.mode"))
	switch mode {
	case "classic":
		return newObcClassic(config, stack)
	case "batch":
		return newObcBatch(config, stack)
	case "sieve":
		return newObcSieve(config, stack)
	default:
		panic(fmt.Errorf("Invalid PBFT mode: %s", mode))
	}
}

func loadConfig() (config *viper.Viper) {
	config = viper.New()

	// for environment variables
	config.SetEnvPrefix(configPrefix)
	config.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	config.SetEnvKeyReplacer(replacer)

	config.SetConfigName("config")
	config.AddConfigPath("./")
	config.AddConfigPath("./openchain/consensus/obcpbft/")
	config.AddConfigPath("../../openchain/consensus/obcpbft")
	err := config.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Error reading %s plugin config: %s", configPrefix, err))
	}
	return
}
