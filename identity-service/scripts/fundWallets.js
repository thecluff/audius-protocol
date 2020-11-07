const Web3 = require('web3')
const web3 = new Web3(new Web3.providers.HttpProvider('https://poa-gateway.staging.audius.co/'))
const EthereumWallet = require('ethereumjs-wallet')
const EthereumTx = require('ethereumjs-tx')

const sourceWallet = {
  publicKey: '0xc981930998b414d2555DC7d17A85591956897Dd1',
  privateKey: '70d220b2370a1bc42ccc8a33dbc6b8494c3f853c12dcc56a0ab944157a1d108f'
}

const receiverWallet = {
  publicKey: '0xdc7f8ff9428b076e45518208e3f4e463cffb579e',
  privateKey: 'e88ae53fcf14fb00d0560a711b1785d3e596b65125437f2a0303b72edb3e8a7e'
}

const MIN_GAS_PRICE = 10 * Math.pow(10, 9)
const HIGH_GAS_PRICE = 25 * Math.pow(10, 9)
const GANACHE_GAS_PRICE = 39062500000
const DEFAULT_GAS_LIMIT = '0xf7100'

const nonce = 602

async function fundWallets () {
  let receipt

  try {
    const privateKeyBuffer = Buffer.from(sourceWallet.privateKey, 'hex')
    const walletAddress = EthereumWallet.fromPrivateKey(privateKeyBuffer)
    const address = walletAddress.getAddressString()

    if (address !== sourceWallet.publicKey.toLowerCase()) {
      throw new Error('Invalid relayerPublicKey')
    }
    const gasPrice = await getGasPrice()
    // const nonce = await web3.eth.getTransactionCount(address)
    const txParams = {
      nonce,
      gasPrice,
      gasLimit: DEFAULT_GAS_LIMIT,
      to: receiverWallet.publicKey,
      value: web3.utils.toHex(web3.utils.toWei(web3.utils.toBN(1), 'ether'))
    }

    const tx = new EthereumTx(txParams)
    tx.sign(privateKeyBuffer) // signing from wallet1

    const signedTx = '0x' + tx.serialize().toString('hex')

    console.log(`txRelay - sending a transaction for wallet1 ${sourceWallet.publicKey} to ${receiverWallet.publicKey}, gasPrice ${parseInt(gasPrice, 16)}, gasLimit ${DEFAULT_GAS_LIMIT}, nonce ${nonce}`)
    receipt = await web3.eth.sendSignedTransaction(signedTx)
  } catch (e) {
    console.error('txRelay - Error in relay', e)
    throw e
  }

  console.log('The receipt:')
  console.log(receipt)
}

async function getGasPrice () {
  let gasPrice = parseInt(await web3.eth.getGasPrice())
  if (isNaN(gasPrice) || gasPrice > HIGH_GAS_PRICE) {
    console.log('txRelay - gas price was not defined or was greater than HIGH_GAS_PRICE', gasPrice)
    gasPrice = GANACHE_GAS_PRICE
  } else if (gasPrice === 0) {
    console.log('txRelay - gas price was zero', gasPrice)
    // If the gas is zero, the txn will likely never get mined.
    gasPrice = MIN_GAS_PRICE
  } else if (gasPrice < MIN_GAS_PRICE) {
    console.log('txRelay - gas price was less than MIN_GAS_PRICE', gasPrice)
    gasPrice = MIN_GAS_PRICE
  }
  gasPrice = '0x' + gasPrice.toString(16)

  return gasPrice
}

fundWallets()
