import * as _lib from './_lib/lib.js'
import {
    Registry,
    UserStorage,
    UserFactory,
    UserReplicaSetManager
} from './_lib/artifacts.js'
import * as _constants from './utils/constants'
const { expectRevert } = require('@openzeppelin/test-helpers');

contract('UserReplicaSetManager', async (accounts) => {
    const deployer = accounts[0]
    const verifierAddress = accounts[2]
    const userId1 = 1
    const userAcct1 = accounts[3]
    const userId2 = 2
    const userAcct2 = accounts[4]
    // First spID = 1, account = accounts[3]
    const cnode1SpID = 1
    const cnode1Account = accounts[5]
    // Second spID = 2, accounts = accounts[4]
    const cnode2SpID = 2
    const cnode2Account = accounts[6]
    // Third spID = 3, accounts = accounts[5]
    const cnode3SpID = 3
    const cnode3Account = accounts[7]
    // Fourth spID = 4, accounts = accounts[6]
    const cnode4SpID = 4
    const cnode4Account = accounts[8]

    // Special permission addresses
    const nodeBootstrapAddress = accounts[12]
    const userReplicaBootstrapAddress = accounts[14]


    const bootstrapSPIds = [cnode1SpID, cnode2SpID, cnode3SpID, cnode4SpID]
    const bootstrapDelegateWallets = [cnode1Account, cnode2Account, cnode3Account, cnode4Account]

    // Contract objects
    let registry
    let userStorage
    let userFactory
    let userReplicaSetManager
    let networkId

    beforeEach(async () => {
        // Initialize contract state
        registry = await Registry.new()
        networkId = Registry.network_id
        // Add user storage and user factory
        userStorage = await UserStorage.new(registry.address)
        await registry.addContract(_constants.userStorageKey, userStorage.address)
        userFactory = await UserFactory.new(registry.address, _constants.userStorageKey, networkId, verifierAddress)
        await registry.addContract(_constants.userFactoryKey, userFactory.address)

        userReplicaSetManager = await UserReplicaSetManager.new(
            registry.address,
            _constants.userFactoryKey,
            userReplicaBootstrapAddress,
            bootstrapSPIds,
            bootstrapDelegateWallets,
            networkId,
            { from: deployer }
        )
        await registry.addContract(_constants.userReplicaSetManagerKey, userReplicaSetManager.address)
        // Initialize users to POA UserFactory
        await registerInitialUsers()
        /*
        // Setup cnode 1 from deployer address
        await addOrUpdateCreatorNode(cnode1SpID, cnode1Account, 0, nodeBootstrapAddress)
        // Setup cnode 2 through cnode1Account
        await addOrUpdateCreatorNode(cnode2SpID, cnode2Account, cnode1SpID, cnode1Account)
        // Setup cnode 3 through cn2Account
        await addOrUpdateCreatorNode(cnode3SpID, cnode3Account, cnode2SpID, cnode2Account)
        // Setup cnode 4 through cn3Account
        await addOrUpdateCreatorNode(cnode4SpID, cnode4Account, cnode3SpID, cnode3Account)
        */
    })

    // Confirm constructor arguments are respected on chain
    let validateBootstrapNodes = async () => {
        // Manually query every constructor spID and confirm matching wallet on chain
        for (var i = 0; i < bootstrapSPIds.length; i++) {
            let spID = bootstrapSPIds[i]
            let cnodeWallet = bootstrapDelegateWallets[i]
            let walletFromChain = await userReplicaSetManager.getContentNodeWallet(spID)
            assert.isTrue(
                cnodeWallet === walletFromChain,
                `Mismatched spID wallet: Expected ${spID} w/wallet ${cnodeWallet}, found ${walletFromChain}`
            )
        }

        // Validate returned arguments from chain match constructor arguments
        let bootstrapIDsFromChain = await userReplicaSetManager.getBootstrapServiceProviderIDs()
        let bootstrapWalletsFromChain = await userReplicaSetManager.getBootstrapServiceProviderDelegateWallets()
        assert.isTrue(
            (bootstrapIDsFromChain.length === bootstrapWalletsFromChain.length) &&
            (bootstrapIDsFromChain.length === bootstrapSPIds.length) &&
            (bootstrapSPIds.length === bootstrapDelegateWallets.length),
            "Unexpected bootstrap constructor argument length returned"
        )
         for (var i = 0; i < bootstrapIDsFromChain.length; i++) {
            assert.isTrue(bootstrapIDsFromChain[i] == bootstrapSPIds[i])
            assert.isTrue(bootstrapWalletsFromChain[i] == bootstrapDelegateWallets[i])
        }
     }

    // Helper Functions
    // Initial 2 users registered to test UserFactory
    let registerInitialUsers = async () => {
        await _lib.addUserAndValidate(
            userFactory,
            userId1,
            userAcct1,
            _constants.testMultihash.digest1,
            _constants.userHandle1,
            true
        )
        await _lib.addUserAndValidate(
            userFactory,
            userId2,
            userAcct2,
            _constants.testMultihash.digest1,
            _constants.userHandle2,
            true
        )
    }

    const toBN = (val) => web3.utils.toBN(val)

    const toBNArray = (bnArray) => { return bnArray.map(x => toBN(x)) }

    /** Helper Functions **/
    let addOrUpdateCreatorNode = async (newCnodeId, newCnodeDelegateOwnerWallet, proposerId, proposerWallet) => {
        await _lib.addOrUpdateCreatorNode(
            userReplicaSetManager,
            newCnodeId,
            newCnodeDelegateOwnerWallet,
            proposerId,
            proposerWallet)
        let walletFromChain = await userReplicaSetManager.getContentNodeWallet(newCnodeId)
        assert.equal(walletFromChain, newCnodeDelegateOwnerWallet, 'Expect wallet assignment')
    }

    let updateReplicaSet = async (userId, newPrimary, newSecondaries, oldPrimary, oldSecondaries, senderAcct) => {
        await _lib.updateReplicaSet(
            userReplicaSetManager,
            userId,
            newPrimary,
            newSecondaries,
            oldPrimary,
            oldSecondaries,
            senderAcct)
        let replicaSetFromChain = await userReplicaSetManager.getUserReplicaSet(userId)
        assert.isTrue(replicaSetFromChain.primary.eq(newPrimary), 'Primary mismatch')
        assert.isTrue(replicaSetFromChain.secondaries.every((replicaId, i) => replicaId.eq(newSecondaries[i])), 'Secondary mismatch')
    }

    /** Test Cases **/
    it('Validate constructor bootstrap arguments', async () => {
        // Confirm constructor arguments validated
        await validateBootstrapNodes()

        // Create an intentionally mismatched length list of bootstrap spIDs<->delegateWallets
        const invalidSPIds = [cnode1SpID, cnode2SpID, cnode3SpID]
        await expectRevert(
            UserReplicaSetManager.new(
                registry.address,
                _constants.userFactoryKey,
                userReplicaBootstrapAddress,
                invalidSPIds,
                bootstrapDelegateWallets,
                networkId,
                { from: deployer }
            ),
            "Mismatched bootstrap array lengths"
        )
    })

    it('Register additional nodes through bootstrap nodes', async () => {
        let newCNodeSPId = 10
        let newCnodeDelegateWallet = accounts[20]
        let invalidProposerId = 12
        let invalidProposerDelegateWallet = accounts[23]
        // Attempt to register from an unknown spID
        await expectRevert(
            addOrUpdateCreatorNode(
                newCNodeSPId,
                newCnodeDelegateWallet,
                invalidProposerId,
                invalidProposerDelegateWallet
            ),
            "Mismatch proposer wallet for existing spID"
        )
        let validProposerId = bootstrapSPIds[2]
        let validBootstrapDelegateWallet = bootstrapDelegateWallets[2]
        // Add from a valid proposer and confirm functionality
        await addOrUpdateCreatorNode(
            newCNodeSPId,
            newCnodeDelegateWallet,
            validProposerId,
            validBootstrapDelegateWallet
        )
    })

    // TODO: Can a given spID <-> delegateWallet update itself?

    // TODO: Validate this scenario - any cnode can ADD another cnode if not present, but ONLY bootstrappers can evict

    it('Configure + update user replica set', async () => {
        let user1Primary = toBN(1)
        let user1Secondaries = toBNArray([2, 3])
        let oldPrimary = user1Primary
        let oldSecondaries = user1Secondaries
        await updateReplicaSet(userId1, user1Primary, user1Secondaries, 0, [], userAcct1)
        // Fail with out of date prior configuration
        await expectRevert(
          updateReplicaSet(userId1, user1Primary, user1Secondaries, 0, [], userAcct1),
          'Invalid prior primary configuration'
        )
        await expectRevert(
          updateReplicaSet(userId1, user1Primary, user1Secondaries, user1Primary, [], userAcct1),
          'Invalid prior secondary configuration'
        )
        // Now issue update from userAcct1
        user1Primary = toBN(2)
        user1Secondaries = toBNArray([3, 1])
        await updateReplicaSet(userId1, user1Primary, user1Secondaries, oldPrimary, oldSecondaries, userAcct1)
        // Swap out secondary cn1 for cn4 from cn3
        oldPrimary = user1Primary
        oldSecondaries = user1Secondaries
        let invalidUser1Secondaries = toBNArray([3, 5])
        // 5 is an invalid ID, confirm failure to update
        await expectRevert(
          updateReplicaSet(userId1, user1Primary, invalidUser1Secondaries, oldPrimary, oldSecondaries, cnode3Account),
          'Secondary must exist'
        )
        user1Secondaries = toBNArray([3, 4])
        // Try to issue an update from the incoming secondary account, confirm failure
        await expectRevert(
          updateReplicaSet(userId1, user1Primary, user1Secondaries, oldPrimary, oldSecondaries, cnode4Account),
          'Invalid update operation'
        )
        await updateReplicaSet(userId1, user1Primary, user1Secondaries, oldPrimary, oldSecondaries, cnode3Account)
    })
})