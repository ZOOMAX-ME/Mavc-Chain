// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//# init --protocol-version 70 --simulator --objects-snapshot-min-checkpoint-lag 2 --reference-gas-price 1337

//# run-jsonrpc
{
  "method": "suix_getReferenceGasPrice",
  "params": []
}
