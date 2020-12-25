const {get, isEmpty, isPlainObject} = require('lodash');
const {v4: uuid4} = require('uuid');

const {dedup} = require('@raychee/utils');


class Identities {

    constructor(logger, name, options, {createIdentityFn, createIdentityError, stored = false} = {}) {
        this.logger = logger;
        this.name = name;
        
        this._identities = {};
        this._stored = stored;
        this._createIdentityFn = createIdentityFn;
        this._createIdentityError = createIdentityError;

        this._createIdentity = dedup(Identities.prototype._createIdentity.bind(this), {key: null});
        this._syncStoreForce = dedup(Identities.prototype._syncStoreForce.bind(this), {queue: 1});
    }

    async _init(store) {
        if (this._stored) {
            store = get(await this.logger.pull(), this.name);
            if (!store) {
                this.logger.crash(
                    'plugin_identities_invalid_name', 'invalid identities name: ', this.name, ', please make sure: ',
                    '1. there is a document in the internal collection service.Store that matches filter {plugin: \'identities\'}, ',
                    '2. there is a valid entry under document field \'data.', this.name, '\''
                );
            }
        }
        this._load(store, {unlock: true});
    }

    _load({options = {}, identities = {}} = {}, {unlock = false} = {}) {
        const minIntervalBetweenStoreUpdate = get(this._options, 'minIntervalBetweenStoreUpdate');
        this._options = this._makeOptions(options);
        if (minIntervalBetweenStoreUpdate !== this._options.minIntervalBetweenStoreUpdate) {
            this._syncStoreForce = dedup(
                Identities.prototype._syncStoreForce.bind(this),
                {within: this._options.minIntervalBetweenStoreUpdate * 1000, queue: 1}
            );
        }
        for (const identity of this._iterIdentities(identities)) {
            if (unlock) {
                identity.locked = null;
            }
            this._add(identity);
        }
    }
    
    _makeOptions(options) {
        return {
            maxDeprecationsBeforeRemoval: 1, minIntervalBetweenUse: 0, minIntervalBetweenStoreUpdate: 10,
            recentlyUsedFirst: true, lockExpire: 10 * 60, maxRetryCreateIdentities: -1, allowNoIdentity: false,
            waitForStoreUpdateWhenNoIdentity: false, pollingIntervalWaitingForAvailable: 1,
            // proxiesOptions: {
            //     identityLocationPreferred: false, identityLocationConstrained: false,
            // },
            ...options,
        };
    }

    _add(identity) {
        identity = {id: uuid4(), deprecated: 0, lastTimeUsed: new Date(), locked: false, ...identity};
        const added = {...identity, ...this._identities[identity.id]};
        this._identities[identity.id] = added;
        return added;
    }
    
    getProxiesOptions() {
        return this._options.proxiesOptions || {};
    }

    async get(logger, {lock = false} = {}) {
        logger = logger || this.logger;
        let identity = undefined, created = true;
        while (!identity && created) {
            for (const i of this._iterIdentities()) {
                if (!this._isAvailable(i)) continue;
                if (!identity) {
                    identity = i;
                } else {
                    if (this._options.recentlyUsedFirst) {
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
            if (!identity) {
                created = await this._createIdentity(logger);
            }
        }
        if (identity) {
            identity = this.touch(logger, identity);
            this._info(logger, identity.id, ' is being used.');
            if (lock) {
                identity = this.lock(logger, identity);
            }
            return identity;
        }
    }
    
    async _createIdentity(logger) {
        logger = logger || this.logger;
        let created = undefined;
        let trial = 0;
        while (true) {
            trial++;
            let error = undefined;
            if (this._createIdentityFn) {
                this._info(logger, 'Create a new identity.');
                try {
                    created = await this._createIdentityFn.call(logger);
                } catch (e) {
                    error = e;
                }
            }
            if (created && (!isPlainObject(created) || !created.data)) {
                this._warn(
                    logger, 'New identity is not valid. Expect a plain object with key "data", got ', created
                );
                created = undefined;
            }
            const logEvent = error ? 'Creating identity failed' : !created ? 'No identity is created' : '';
            let logReason = !this._createIdentityFn ? [': No createIdentityFn is provided.'] : ['.'];
            if (error) {
                let isRetryableError = false;
                if (error instanceof Error && error.name === 'JobRuntime') {
                    logReason = [': ', error];
                    isRetryableError = true;
                } else if (this._createIdentityError) {
                    let message = undefined;
                    try {
                        message = await this._createIdentityError.call(logger, error);
                    } catch (ee) {
                        logger.warn('Another error occurred in createIdentityError(): ', ee);
                    }
                    if (message) {
                        logReason = [': ', ...(Array.isArray(message) ? message : [message])];
                        isRetryableError = true;
                    }
                }
                if (!isRetryableError) {
                    throw error;
                }
            } else if (created) {
                const added = this._add(created);
                this._info(logger, 'New identity is created: ', added.id, ' ', added.data);
                return true;
            }
            if (this._options.maxRetryCreateIdentities >= 0 && trial <= this._options.maxRetryCreateIdentities && this._createIdentityFn) {
                this._warn(
                    logger, logEvent, ', will re-try (', trial, '/',
                    this._options.maxRetryCreateIdentities, ')', ...logReason
                );
            } else {
                if (this._options.allowNoIdentity) {
                    this._warn(
                        logger, logEvent,
                        ...(this._options.maxRetryCreateIdentities >= 0 && this._createIdentityFn ? [
                            ' and too many retries have been performed (',
                            this._options.maxRetryCreateIdentities, '/', this._options.maxRetryCreateIdentities, ')'
                        ] : []),
                        ', no identity would be used', ...logReason
                    );
                    return false;
                } else {
                    if (!isEmpty(this._identities) && this._options.pollingIntervalWaitingForAvailable > 0) {
                        this._info(
                            logger, 'Sleep ', this._options.pollingIntervalWaitingForAvailable,
                            ' seconds to wait for existing identities to become available.'
                        );
                        await logger.sleep(this._options.pollingIntervalWaitingForAvailable);
                        return true;
                    } else if (isEmpty(this._identities) && this._stored && this._options.waitForStoreUpdateWhenNoIdentity) {
                        const store = await this.logger.pull({
                            waitUntil: s => {
                                const identities = get(s, [this.name, 'identities']);
                                return !isEmpty(identities);
                            },
                            message: `waiting for a valid identity in store field ${this.name}`
                        }, logger);
                        this._load(store[this.name]);
                        return true;
                    } else {
                        this._fail(
                            logger, 'plugin_identities_create_error', logEvent,
                            ...(this._options.maxRetryCreateIdentities >= 0 && this._createIdentityFn ? [
                                ' and too many retries have been performed (',
                                this._options.maxRetryCreateIdentities, '/', this._options.maxRetryCreateIdentities, ')'
                            ] : []),
                            ...logReason
                        );
                    }
                }
            }
        }
    }

    lock(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        this._info(logger, id, ' is locked.');
        identity.locked = new Date();
        return {id, ...identity};
    }

    unlock(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        if (identity.locked) {
            this._info(logger, id, ' is unlocked.');
            identity.locked = null;
            this.touch(logger, id);
        }
        return {id, ...identity};
    }

    touch(_, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        identity.lastTimeUsed = new Date();
        this._syncStore();
        return {id, ...identity};
    }

    update(_, one, data) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        identity.data = data;
        this._syncStore();
        return {id, ...identity};
    }

    renew(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        if (identity.deprecated > 0) {
            this._info(logger, id, ' is renewed.');
            identity.deprecated = 0;
            this._syncStore();
        }
        return {id, ...identity};
    }

    deprecate(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        identity.deprecated = (identity.deprecated || 0) + 1;
        this._info(
            logger, id, ' is deprecated (', identity.deprecated, '/',
            this._options.maxDeprecationsBeforeRemoval >= 0 ? this._options.maxDeprecationsBeforeRemoval : '∞', 
            ').'
        );
        if (this._options.maxDeprecationsBeforeRemoval >= 0 && identity.deprecated >= this._options.maxDeprecationsBeforeRemoval) {
            this.remove(logger, id);
        }
        this.unlock(logger, id);
        this._syncStore();
        return {id, ...identity};
    }

    remove(logger, one) {
        const {id, identity} = this._find(one);
        if (!identity) return;
        this._identities[id] = null;
        this._info(logger, id, ' is removed: ', identity);
        this._syncStore();
        return {id, ...identity};
    }

    *_iterIdentities(identities) {
        for (const [id, identity] of Object.entries(identities || this._identities)) {
            if (!identity) continue;
            yield {id, ...identity};
        }
    }
    
    _find(one) {
        if (typeof one === "string") {
            return {id: one, identity: this._identities[one]};
        } else {
            return {id: one.id, identity: this._identities[one.id] || {...one}};
        }
    }

    _syncStore() {
        this._syncStoreForce().catch(e => console.error('This should never happen: ', e));
    }

    async _syncStoreForce() {
        let deleteNullIdentities = true;
        if (this._stored) {
            try {
                let store;
                if (isEmpty(this._identities)) {
                    store = await this.logger.pull();
                } else {
                    store = await this.logger.push({[this.name]: {identities: this._identities}});
                }
                this._load(store[this.name]);
            } catch (e) {
                deleteNullIdentities = false;
                this._warn(undefined, 'Sync identities of name ', this.name, ' failed: ', e);
            }
        }
        if (deleteNullIdentities) {
            Object.entries(this._identities)
                .filter(([, i]) => !i)
                .forEach(([id]) => delete this._identities[id]);
        }
    }

    _isAvailable(identity) {
        const now = Date.now();
        return (!identity.locked || identity.locked < now - this._options.lockExpire * 1000) &&
            identity.lastTimeUsed <= now - this._options.minIntervalBetweenUse * 1000;
    }
    
    _logPrefix() {
        return this.name ? `Identities ${this.name}: ` : 'Identities: '
    }

    _info(logger, ...args) {
        (logger || this.logger).info(this._logPrefix(), ...args);
    }

    _warn(logger, ...args) {
        (logger || this.logger).warn(this._logPrefix(), ...args);
    }
    
    _fail(logger, code, ...args) {
        (logger || this.logger).fail(code, this._logPrefix(), ...args);
    }

    _crash(logger, code, ...args) {
        (logger || this.logger).crash(code, this._logPrefix(), ...args);
    }

}


module.exports = {
    type: 'identities',
    key({name}) {
        return name;
    },
    async create({name, options, createIdentityFn, createIdentityError, stored = false}) {
        const identities = new Identities(this, name, options, {createIdentityFn, createIdentityError, stored});
        await identities._init({options});
        return identities;
    },
    async destroy(identities) {
        await identities._syncStoreForce();
    }
};
