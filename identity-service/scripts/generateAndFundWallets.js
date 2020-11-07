const Web3 = require('web3')
const web3 = new Web3(new Web3.providers.HttpProvider('https://poa-gateway.audius.co'))
const crypto = require('crypto')
const EthereumWallet = require('ethereumjs-wallet')
const EthereumTx = require('ethereumjs-tx')
const BN = require('bn.js')

const DEFAULT_GAS_LIMIT = '0xf7100'
const DISTRIBUTE_TOKENS_AMOUNT = 2
const MIN_GAS_PRICE = 10 * Math.pow(10, 9)
const HIGH_GAS_PRICE = 25 * Math.pow(10, 9)
const GANACHE_GAS_PRICE = 39062500000
const numWallets = 49

// Manually update
const relayerWallet = {
  publicKey: '',
  privateKey: ''
}

async function generateWallets () {
  let wallets = []
  for (let i = 0; i < numWallets; i++) {
    const privateKey = crypto.randomBytes(32).toString('hex')
    const privateKeyBuffer = Buffer.from(privateKey, 'hex')
    const walletObj = EthereumWallet.fromPrivateKey(privateKeyBuffer)
    let info = {
      publicKey: walletObj.getAddressString(),
      privateKey
    }
    wallets.push(info)
  }

  return wallets
}

async function relayerHasEnoughFunds () {
  const relayerBalance = await web3.eth.getBalance(relayerWallet.publicKey)
  const balanceBN = new BN(relayerBalance)
  const amtBN = new BN(DISTRIBUTE_TOKENS_AMOUNT)
  const enoughFunds = balanceBN.gte(amtBN)

  return { enoughFunds, balance: relayerBalance }
}

async function fundWallets (receiverWallets) {
  const privateKeyBuffer = Buffer.from(relayerWallet.privateKey, 'hex')
  const walletAddress = EthereumWallet.fromPrivateKey(privateKeyBuffer)
  const address = walletAddress.getAddressString()

  if (address !== relayerWallet.publicKey.toLowerCase()) {
    throw new Error('Invalid relayerPublicKey')
  }

  let receipt
  for (const receiverWallet of receiverWallets) {
    try {
      const { enoughFunds, balance } = await relayerHasEnoughFunds()
      if (!enoughFunds) {
        throw new Error(`Relayer wallet does not have enough funds (currently at [${balance}])`)
      }

      const gasPrice = await getGasPrice()
      const nonce = await web3.eth.getTransactionCount(address)
      const txParams = {
        nonce,
        gasPrice,
        gasLimit: DEFAULT_GAS_LIMIT,
        to: receiverWallet.publicKey,
        value: web3.utils.toHex(web3.utils.toWei(web3.utils.toBN(DISTRIBUTE_TOKENS_AMOUNT), 'ether'))
      }

      const tx = new EthereumTx(txParams)
      tx.sign(privateKeyBuffer) // signing from wallet1

      const signedTx = '0x' + tx.serialize().toString('hex')

      console.log(`txRelay - sending a transaction for wallet1 ${relayerWallet.publicKey} to ${receiverWallet.publicKey}, gasPrice ${parseInt(gasPrice, 16)}, gasLimit ${DEFAULT_GAS_LIMIT}, nonce ${nonce}`)
      receipt = await web3.eth.sendSignedTransaction(signedTx)
    } catch (e) {
      // console.error('txRelay - Error in relay', e)
      console.log(`funding error :( - ${e}`)
      throw e
    }

    console.log('The receipt:')
    console.log(receipt)
  }
}

// Query mainnet ethereum gas prices
async function getGasPrice (logger) {
  let gasPrice = parseInt(await web3.eth.getGasPrice())
  if (isNaN(gasPrice) || gasPrice > HIGH_GAS_PRICE) {
    console.log('getGasPrice - gas price was not defined or was greater than HIGH_GAS_PRICE', gasPrice)
    gasPrice = GANACHE_GAS_PRICE
  } else if (gasPrice === 0) {
    console.log('getGasPrice - gas price was zero', gasPrice)
    // If the gas is zero, the txn will likely never get mined.
    gasPrice = MIN_GAS_PRICE
  } else if (gasPrice < MIN_GAS_PRICE) {
    console.log('getGasPrice - gas price was less than MIN_GAS_PRICE', gasPrice)
    gasPrice = MIN_GAS_PRICE
  }
  gasPrice = '0x' + gasPrice.toString(16)

  return gasPrice
}

async function run () {
  const wallets = await generateWallets()
  console.log('The wallets:', wallets)
  await fundWallets(wallets)
  // await fundWallets([
  //   {
  //     publicKey: '0x2408da8a1d8ed3d9f9b59e07f5e5a98be476656b',
  //     privateKey: 'ca47e459081c00d51182a326de5f481904e47366c535c0212d30755882f0a140'
  //   }
  // ])

  console.log(JSON.stringify(wallets))
}

run()
