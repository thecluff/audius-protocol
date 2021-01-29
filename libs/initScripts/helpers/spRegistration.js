const DelegateManagerClient = require('../../src/services/ethContracts/delegateManagerClient')
const { getEthWeb3AndAccounts, convertAudsToWeiBN } = require('./utils')

/**
 *
 * @param {Object} audiusLibs fully formed audius libs instance with eth contracts connection
 */
async function getStakingParameters (audiusLibs) {
  if (!audiusLibs) throw new Error('audiusLibs is not defined')

  let min = await audiusLibs.ethContracts.StakingProxyClient.getMinStakeAmount()
  let max = await audiusLibs.ethContracts.StakingProxyClient.getMaxStakeAmount()
  console.log(`getStakingParameters: min: ${min}, max: ${max}`)
  return { min, max }
}

/**
 * Local only
 * @param {Object} audiusLibs fully formed audius libs instance with eth contracts connection
 * @param {String} serviceType service type trying to register
 * @param {String} serviceEndpoint url string of service to register
 * @param {Number} amountOfAUDS integer amount of auds tokens
 */
async function registerLocalService (audiusLibs, serviceType, serviceEndpoint, amountOfAUDS) {
  if (!audiusLibs) throw new Error('audiusLibs is not defined')
  if (!amountOfAUDS) throw new Error('registerLocalService requires an amountOfAuds property')

  const { ethWeb3 } = await getEthWeb3AndAccounts(audiusLibs)
  console.log('\nregistering service providers/---')
  let initialTokenInAudWeiBN = convertAudsToWeiBN(ethWeb3, amountOfAUDS)

  try {
    // Register service
    console.log(`registering service ${serviceType} ${serviceEndpoint}`)
    let tx = await audiusLibs.ethContracts.ServiceProviderFactoryClient.register(
      serviceType,
      serviceEndpoint,
      initialTokenInAudWeiBN)
    console.log(`registered service ${serviceType} ${serviceEndpoint} - ${tx.txReceipt.transactionHash}`)
  } catch (e) {
    if (!e.toString().includes('already registered')) {
      console.log(e)
    } else {
      console.log(`\n${serviceEndpoint} already registered`)
    }
  }
}

/**
 * Local only
 * @param {Object} audiusLibs fully formed audius libs instance with eth contracts connection
 * @param {String} serviceType service type trying to register
 */
async function registerLocalServiceType (audiusLibs, serviceType) {
  if (!audiusLibs) throw new Error('audiusLibs is not defined')
  const minAuds = '200000'
  const maxAuds = '200000000'
  const { ethWeb3 } = await getEthWeb3AndAccounts(audiusLibs)
  let min = convertAudsToWeiBN(ethWeb3, minAuds)
  let max = convertAudsToWeiBN(ethWeb3, maxAuds)

  try {
    // Register service
    console.log(`registering service type ${serviceType}`)
    let tx = await audiusLibs.ethContracts.ServiceTypeManagerClient.addServiceType(
      serviceType,
      min,
      max)
    console.log(`registered service type ${serviceType}`)
  } catch (e) {
    if (!e.toString().includes('already registered')) {
      console.log(e)
    } else {
      console.log(`\n${serviceEndpoint} already registered`)
    }
  }
}

/**
 * Local only
 * @param {Object} audiusLibs fully formed audius libs instance with eth contracts connection
 * @param {Number} amountOfAUDS integer amount of auds tokens
 */
async function increaseStake (audiusLibs, amountOfAUDS) {
  if (!audiusLibs) throw new Error('audiusLibs is not defined')
  if (!amountOfAUDS) throw new Error('registerLocalService requires an amountOfAuds property')

  const { ethWeb3 } = await getEthWeb3AndAccounts(audiusLibs)
  console.log('\nIncreaseing Stake/---')
  console.log({ ethWeb3 })
  let ownerWallet = audiusLibs.ethWeb3Config.ownerWallet
  console.log(ownerWallet)
  let tokenAmountInAudWeiBN = convertAudsToWeiBN(ethWeb3, amountOfAUDS)

  try {
    // Register service
    console.log(`Increaseing Stake by: ${amountOfAUDS}`)
    let tx1 = await audiusLibs.ethContracts.StakingProxyClient.totalStakedFor(ownerWallet)
    console.log(tx1.toString())
    let tx = await audiusLibs.ethContracts.ServiceProviderFactoryClient.increaseStake(tokenAmountInAudWeiBN)
    console.log(`Increased Stake - ${tx.txReceipt.transactionHash}`)
  } catch (e) {
    console.log(e)
  }
}


/**
 * Local only
 * @param {Object} audiusLibs fully formed audius libs instance with eth contracts connection
 * @param {String} serviceType service type trying to register
 * @param {String} serviceEndpoint url string of service to register
 */
async function deregisterLocalService (audiusLibs, serviceType, serviceEndpoint) {
  if (!audiusLibs) throw new Error('audiusLibs is not defined')

  try {
    // de-register service
    console.log(`\nde-registering service ${serviceType} ${serviceEndpoint}`)
    let tx = await audiusLibs.ethContracts.ServiceProviderFactoryClient.deregister(
      serviceType,
      serviceEndpoint)
    console.dir(tx, { depth: 5 })
  } catch (e) {
    console.log(e)
  }
}

/**
 * Local only
 * @param {Object} audiusLibs fully formed audius libs instance with eth contracts connection
 */
async function queryLocalServices (audiusLibs, serviceTypeList) {
  if (!audiusLibs) throw new Error('audiusLibs is not defined')

  const { ethAccounts } = await getEthWeb3AndAccounts(audiusLibs)

  for (const spType of serviceTypeList) {
    console.log(`\n${spType}`)
    let spList = await audiusLibs.ethContracts.ServiceProviderFactoryClient.getServiceProviderList(spType)
    for (const sp of spList) {
      console.log(sp)
      const { spID, type, endpoint } = sp
      let idFromEndpoint =
        await audiusLibs.ethContracts.ServiceProviderFactoryClient.getServiceProviderIdFromEndpoint(endpoint)
      console.log(`ID from endpoint: ${idFromEndpoint}`)
      let infoFromId =
        await audiusLibs.ethContracts.ServiceProviderFactoryClient.getServiceEndpointInfo(type, spID)
      let jsonInfoFromId = JSON.stringify(infoFromId)
      console.log(`Info from ID: ${jsonInfoFromId}`)
      let idsFromAddress =
        await audiusLibs.ethContracts.ServiceProviderFactoryClient.getServiceProviderIdsFromAddress(
          ethAccounts[0],
          type)
      console.log(`SP IDs from owner wallet ${ethAccounts[0]}: ${idsFromAddress}`)
    }
    let numProvs = await audiusLibs.ethContracts.ServiceProviderFactoryClient.getTotalServiceTypeProviders(spType)
    console.log(`num ${spType}: ${numProvs}`)
  }
  console.log('----querying service providers done')
}

/**
 * Local only
 * @param {Object} audiusLibs fully formed audius libs instance with eth contracts connection
 */
async function reduceLockupDurations (audiusLibs) {
  if (!audiusLibs) throw new Error('audiusLibs is not defined')
  try {

    await audiusLibs.ethContracts.GovernanceClient.setVotingPeriod(10)
    await audiusLibs.ethContracts.GovernanceClient.setMaxInProgressProposals(20)
    await audiusLibs.ethContracts.GovernanceClient.setExecutionDelay(10)
    await audiusLibs.ethContracts.ServiceProviderFactoryClient.updateDecreaseStakeLockupDuration(21)
    await audiusLibs.ethContracts.ClaimsManagerClient.updateFundingRoundBlockDiff(2)
    await audiusLibs.ethContracts.ServiceProviderFactoryClient.updateDeployerCutLockupDuration(11)
    await audiusLibs.ethContracts.DelegateManagerClient.updateUndelegateLockupDuration(21)
    await audiusLibs.ethContracts.DelegateManagerClient.updateRemoveDelegatorLockupDuration(100)
  } catch (err) {
    console.log('err', err)
  }
}



module.exports = { 
  getStakingParameters,
  registerLocalService,
  deregisterLocalService,
  queryLocalServices,
  increaseStake,
  registerLocalServiceType,
  reduceLockupDurations
}
