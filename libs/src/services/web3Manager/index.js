const Web3 = require('../../web3')
const sigUtil = require('eth-sig-util')
const retry = require('async-retry')
const AudiusABIDecoder = require('../ABIDecoder/index')
const EthereumWallet = require('ethereumjs-wallet')
let XMLHttpRequestRef
if (typeof window === 'undefined' || window === null) {
  XMLHttpRequestRef = require('xmlhttprequest').XMLHttpRequest
} else {
  XMLHttpRequestRef = window.XMLHttpRequest
}

const DEFAULT_GAS_AMOUNT = 1011968

/** singleton class to be instantiated and persisted with every AudiusLibs */
class Web3Manager {
  constructor (web3Config, identityService, hedgehog, isServer) {
    this.web3Config = web3Config
    this.isServer = isServer

    // Unset if externalWeb3 = true
    this.identityService = identityService
    this.hedgehog = hedgehog
    this.AudiusABIDecoder = AudiusABIDecoder
  }

  async init () {
    const web3Config = this.web3Config
    if (!web3Config) throw new Error('Failed to initialize Web3Manager')

    if (
      // External Web3
      web3Config &&
      web3Config.useExternalWeb3 &&
      web3Config.externalWeb3Config &&
      web3Config.externalWeb3Config.web3 &&
      web3Config.externalWeb3Config.ownerWallet
    ) {
      this.web3 = web3Config.externalWeb3Config.web3
      this.useExternalWeb3 = true
      this.ownerWallet = web3Config.externalWeb3Config.ownerWallet
    } else if (
      // Internal Web3
      web3Config &&
      !web3Config.useExternalWeb3 &&
      web3Config.internalWeb3Config &&
      web3Config.internalWeb3Config.web3ProviderEndpoints
    ) {
      // either user has external web3 but it's not configured, or doesn't have web3
      this.web3 = new Web3(this.provider(web3Config.internalWeb3Config.web3ProviderEndpoints[0], 10000))
      this.useExternalWeb3 = false

      if (web3Config.internalWeb3Config.privateKey) {
        let pkeyBuffer = Buffer.from(web3Config.internalWeb3Config.privateKey, 'hex')
        this.ownerWallet = EthereumWallet.fromPrivateKey(pkeyBuffer)
        return
      }

      // create private key pair here if it doesn't already exist
      const storedWallet = this.hedgehog.getWallet()
      if (storedWallet) {
        this.ownerWallet = storedWallet
      } else {
        const passwordEntropy = `audius-dummy-pkey-${Math.floor(Math.random() * 1000000)}`
        this.ownerWallet = await this.hedgehog.createWalletObj(passwordEntropy)
      }
    } else {
      throw new Error("web3ProviderEndpoint isn't passed into constructor")
    }
  }

  getWeb3 () {
    return this.web3
  }

  setWeb3 (web3) {
    this.web3 = web3
  }

  getWalletAddress () {
    if (this.useExternalWeb3) {
      // Lowercase the owner wallet. Consider using the checksum address.
      // See https://github.com/ethereum/EIPs/blob/master/EIPS/eip-55.md.
      return this.ownerWallet.toLowerCase()
    } else {
      return this.ownerWallet.getAddressString()
    }
  }

  setOwnerWallet (ownerWallet) {
    this.ownerWallet = ownerWallet
  }

  web3IsExternal () {
    return this.useExternalWeb3
  }

  getOwnerWalletPrivateKey () {
    if (this.useExternalWeb3) {
      throw new Error("Can't get owner wallet private key for external web3")
    } else {
      return this.ownerWallet.getPrivateKey()
    }
  }

  /**
   * Signs provided string data (should be timestamped).
   * @param {string} data
   */
  async sign (data) {
    if (this.useExternalWeb3) {
      const account = this.getWalletAddress()
      if (this.isServer) {
        return this.web3.eth.sign(this.web3.utils.fromUtf8(data), account)
      } else {
        return this.web3.eth.personal.sign(this.web3.utils.fromUtf8(data), account)
      }
    }

    return sigUtil.personalSign(this.getOwnerWalletPrivateKey(), { data })
  }

  /**
   * Given a data payload and signature, verifies that signature is valid, and returns
   * Ethereum wallet address used to sign data.
   * @param {string} data information that was signed
   * @param {string} signature hex-formatted signature of data generated by web3 personalSign method
   */
  async verifySignature (data, signature) {
    return sigUtil.recoverPersonalSignature({ data: data, sig: signature })
  }

  async signTypedData (signatureData) {
    if (this.useExternalWeb3) {
      return ethSignTypedData(
        this.getWeb3(),
        this.getWalletAddress(),
        signatureData
      )
    } else {
      // Due to changes in ethereumjs-util's toBuffer method as of v6.2.0
      // non hex-prefixed string values are not permitted and need to be
      // provided directly as a buffer.
      // https://github.com/ethereumjs/ethereumjs-util/releases/tag/v6.2.0
      Object.keys(signatureData.message).forEach(key => {
        if (
          typeof signatureData.message[key] === 'string' &&
          !signatureData.message[key].startsWith('0x')
        ) {
          signatureData.message[key] = Buffer.from(signatureData.message[key])
        }
      })
      return sigUtil.signTypedData(
        this.ownerWallet.getPrivateKey(),
        { data: signatureData }
      )
    }
  }

  getWalletAddressString () {
    if (this.useExternalWeb3) {
      return this.ownerWallet
    } else {
      return this.ownerWallet.getAddressString()
    }
  }

  async sendTransaction (
    contractMethod,
    contractRegistryKey,
    contractAddress,
    txGasLimit = DEFAULT_GAS_AMOUNT,
    txRetries = 5
  ) {
    if (this.useExternalWeb3) {
      return contractMethod.send(
        { from: this.ownerWallet, gas: txGasLimit }
      )
    } else {
      const encodedABI = contractMethod.encodeABI()

      const response = await retry(async () => {
        return this.identityService.relay(
          contractRegistryKey,
          contractAddress,
          this.ownerWallet.getAddressString(),
          encodedABI,
          txGasLimit
        )
      }, {
        // Retry function 5x by default
        // 1st retry delay = 500ms, 2nd = 1500ms, 3rd...nth retry = 4000 ms (capped)
        minTimeout: 500,
        maxTimeout: 4000,
        factor: 3,
        retries: txRetries,
        onRetry: (err, i) => {
          if (err) {
            console.log(`Retry error : ${err}`)
          }
        }
      })

      const receipt = response['receipt']

      // interestingly, using contractMethod.send from Metamask's web3 (eg. like in the if
      // above) parses the event log into an 'events' key on the transaction receipt and
      // blows away the 'logs' key. However, using sendRawTransaction as our
      // relayer does, returns only the logs. Here, we replicate the part of the 'events'
      // key that our code consumes, but we may want to change our functions to consume
      // this data in a different way in future (this parsing is messy).
      // More on Metamask's / Web3.js' behavior here:
      // https://web3js.readthedocs.io/en/1.0/web3-eth-contract.html#methods-mymethod-send
      if (receipt.logs) {
        const events = {}
        const decoded = this.AudiusABIDecoder.decodeLogs(contractRegistryKey, receipt.logs)
        decoded.forEach((evt) => {
          const returnValues = {}
          evt.events.forEach((arg) => {
            returnValues[arg['name']] = arg['value']
          })
          events[evt['name']] = { returnValues }
        })
        receipt['events'] = events
      }
      return response['receipt']
    }
  }

  // TODO - Remove this. Adapted from https://github.com/raiden-network/webui/pull/51/files
  // Vendored code below
  provider (url, timeout) {
    return this.monkeyPatchProvider(new Web3.providers.HttpProvider(url, timeout))
  }

  // TODO: Workaround for https://github.com/ethereum/web3.js/issues/1803 it should be immediately removed
  // as soon as the issue is fixed upstream.
  // Issue is also documented here https://github.com/ethereum/web3.js/issues/1802
  monkeyPatchProvider (httpProvider) {
    override(httpProvider, '_prepareRequest', function () {
      return function () {
        const request = new XMLHttpRequestRef()

        request.open('POST', this.host, true)
        request.setRequestHeader('Content-Type', 'application/json')
        request.timeout = this.timeout && this.timeout !== 1 ? this.timeout : 0

        if (this.headers) {
          this.headers.forEach(function (header) {
            request.setRequestHeader(header.name, header.value)
          })
        }
        return request
      }
    })
    return httpProvider
  }
  // End vendored code
}

module.exports = Web3Manager

/** Browser and testing-compatible signTypedData */
const ethSignTypedData = (web3, wallet, signatureData) => {
  return new Promise((resolve, reject) => {
    let method
    if (web3.currentProvider.isMetaMask === true) {
      method = 'eth_signTypedData_v3'
      signatureData = JSON.stringify(signatureData)
    } else {
      method = 'eth_signTypedData'
      // fix per https://github.com/ethereum/web3.js/issues/1119
    }

    web3.currentProvider.send({
      method: method,
      params: [wallet, signatureData],
      from: wallet
    }, (err, result) => {
      if (err) {
        reject(err)
      } else if (result.error) {
        reject(result.error)
      } else {
        resolve(result.result)
      }
    })
  })
}


function override (object, methodName, callback) {
  object[methodName] = callback(object[methodName])
}
