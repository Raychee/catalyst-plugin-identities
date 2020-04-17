const {get, isEmpty} = require('lodash');
const {v4: uuid4} = require('uuid');

const {dedup} = require('@raychee/utils');


class Identities {

    constructor(logger, name, options, stored = false) {
        this.logger = logger;
        this.name = name;
        this.stored = stored;
        this.identities = {};
        if (!stored) {
            this._load({options});
        }

        this._init = dedup(Identities.prototype._init.bind(this));
        this._get = dedup(Identities.prototype._get.bind(this), {key: null});
        this.__syncStore = dedup(Identities.prototype._syncStoreForce.bind(this));
    }

    async _init() {
        if (this.stored) {
            const store = get(await this.logger.pull(), this.name);
            if (!store) {
                this.logger.crash(
                    '_identities_crash', 'invalid identities name: ', this.name, ', please make sure: ',
                    '1. there is a document in the internal table service.Store that matches filter {plugin: \'identities\'}, ',
                    '2. there is a valid identities options entry under document field \'data.', this.name, '\''
                );
            }
            this._load(store);
        }
    }

    _load({options = {}, identities = {}} = {}) {
        const minIntervalBetweenStoreUpdate = get(this.options, 'minIntervalBetweenStoreUpdate');
        // {
        //     createIdentityFn,
        //     maxDeprecationsBeforeRemoval = 1, minIntervalBetweenUse = 0, recentlyUsedFirst = true,
        //     minIntervalBetweenStoreUpdate = 10, lockExpire = 10 * 60,
        // } = options;
        this.options = this._makeOptions(options);
        if (minIntervalBetweenStoreUpdate !== this.options.minIntervalBetweenStoreUpdate) {
            this.__syncStore = dedup(
                Identities.prototype._syncStoreForce.bind(this),
                {within: this.options.minIntervalBetweenStoreUpdate * 1000}
            );
        }
        for (const identity of this._iterIdentities(identities)) {
            delete identity.locked;
            this._add(identity);
        }
    }

    _add({id = uuid4(), data, deprecated = 0, lastTimeUsed = new Date(0), locked = false}) {
        const identity = {data, deprecated, lastTimeUsed, locked};
        this.identities[id] = {...identity, ...this.identities[id]};
        return {id, ...identity};
    }

    async get(logger, {ifAbsent = undefined, waitForStore = false, lock = false} = {}) {
        logger = logger || this.logger;
        while (true) {
            let identity = undefined;
            for (const i of this._iterIdentities()) {
                if (!this._isAvailable(i)) continue;
                if (!identity) {
                    identity = i;
                } else {
                    if (this.options.recentlyUsedFirst) {
                        if (i.lastTimeUsed > identity.lastTimeUsed) {
                            identity = i;
                        }
                    } else {
                        if (i.lastTimeUsed < identity.lastTimeUsed) {
                            identity = i;
                        }
                    }
                }
            }
            if (identity) {
                this.touch(logger, identity);
                this._info(logger, identity.id, ' is being used.');
                if (lock) {
                    this.lock(logger, identity);
                }
                return identity;
            }
            await this._get(logger, ifAbsent || this.options.createIdentityFn, waitForStore);
        }
    }

    async _get(logger, createIdentityFn, waitForStore) {
        let identity = undefined;
        if (createIdentityFn) {
            identity = await createIdentityFn();
            if (identity) {
                identity = this._add(identity);
            }
        }
        if (!identity && waitForStore && this.stored) {
            const store = await this.logger.pull({
                waitUntil: s => {
                    const identities = get(s, [this.name, 'identities'], {});
                    for (const identity of this._iterIdentities(identities)) {
                        if (this._isAvailable(identity)) return true;
                    }
                    return false;
                },
                message: `waiting for a valid identity in store field ${this.name}`
            });
            this._load(store[this.name]);
        }
    }

    lock(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        this._info(logger, id, ' is locked.');
        identity.locked = new Date();
    }

    unlock(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        if (identity.locked) {
            this._info(logger, id, ' is unlocked.');
            identity.locked = undefined;
            this.touch(logger, id);
        }
    }

    touch(_, one) {
        const {identity} = this._find(one);
        if (!identity) return;
        identity.lastTimeUsed = new Date();
        this._syncStore();
    }

    update(_, one, data) {
        const {identity} = this._find(one);
        if (!identity) return;
        identity.data = data;
        this._syncStore();
    }

    renew(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        if (identity.deprecated > 0) {
            this._info(logger, id, ' is renewed.');
            identity.deprecated = 0;
            this._syncStore();
        }
    }

    deprecate(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        identity.deprecated = (identity.deprecated || 0) + 1;
        this._info(
            logger, id, ' is deprecated (', identity.deprecated, '/',
            this.options.maxDeprecationsBeforeRemoval, ').'
        );
        if (identity.deprecated >= this.options.maxDeprecationsBeforeRemoval) {
            this.remove(logger, id);
        }
        this.unlock(logger, id);
        this._syncStore();
    }

    remove(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        this.identities[id] = null;
        this._info(logger, id, ' is removed: ', identity);
        this._syncStore();
    }

    *_iterIdentities(identities) {
        for (const [id, identity] of Object.entries(identities || this.identities)) {
            if (!identity) continue;
            yield {id, ...identity};
        }
    }

    _id(one) {
        return typeof one === "string" ? one : one.id;
    }

    _find(one) {
        const id = this._id(one);
        return {id, identity: this.identities[id]};
    }

    _syncStore() {
        this.__syncStore().catch(e => console.error('This should never happen: ', e));
    }

    async _syncStoreForce() {
        let deleteNullIdentities = true;
        if (this.stored) {
            try {
                let store;
                if (isEmpty(this.identities)) {
                    store = await this.logger.pull();
                } else {
                    store = await this.logger.push({[this.name]: {identities: this.identities}});
                }
                this._load(store[this.name]);
            } catch (e) {
                deleteNullIdentities = false;
                this._warn(undefined, 'Sync identities of name ', this.name, ' failed: ', e);
            }
        }
        if (deleteNullIdentities) {
            Object.entries(this.identities)
                .filter(([, i]) => !i)
                .forEach(([id]) => delete this.identities[id]);
        }
    }

    _isAvailable(identity) {
        const now = Date.now();
        return (!identity.locked || identity.locked < now - this.options.lockExpire * 1000) &&
            identity.lastTimeUsed <= now - this.options.minIntervalBetweenUse * 1000;
    }

    _makeOptions(options) {
        return {
            createIdentityFn: (this.options || {}).createIdentityFn,
            maxDeprecationsBeforeRemoval: 1, minIntervalBetweenUse: 0, minIntervalBetweenStoreUpdate: 10,
            recentlyUsedFirst: true, lockExpire: 10 * 60,
            ...options,
        };
    }

    _info(logger, ...args) {
        (logger || this.logger).info(this.name ? `Identities ${this.name}: ` : 'Identities: ', ...args);
    }

    _warn(logger, ...args) {
        (logger || this.logger).warn(this.name ? `Identities ${this.name}: ` : 'Identities: ', ...args);
    }

}


module.exports = {
    type: 'identities',
    key({name}) {
        return name;
    },
    async create({name, options, stored = false}) {
        const identities = new Identities(this, name, options, stored);
        await identities._init();
        return identities;
    },
    async destroy(identities) {
        await identities._syncStoreForce();
    }
};