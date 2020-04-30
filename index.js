const blacklist = require('./blacklist');
const whitelist = require('./whitelist');
const axios = require('axios');
const Readability = require('readability');
const isProbablyReaderable = require('readability/Readability-readerable').isProbablyReaderable
const jsdom = require("jsdom");
const { JSDOM } = jsdom;

const {Datastore} = require('@google-cloud/datastore');
const datastore = new Datastore();

const {PubSub} = require('@google-cloud/pubsub');
const pubSub = new PubSub();
const newLinkPublisher = pubSub.topic(process.env.NEW_LINK_TOPIC, {
    batching: {
        maxMessages: 100,
        maxMilliseconds: 1000,
    },
});
const newArticlePublisher = pubSub.topic(process.env.NEW_ARTICLE_TOPIC);


/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.crawler = async (event, context) => {
    return;
    const url = Buffer.from(event.data, 'base64').toString();
    if (!event.attributes || !event.attributes.parentUrl || !isLinkWhiteListed(event.attributes.parentUrl)) {
        console.log('parent url not whitelisted', event.attributes.parentUrl);
        return;
    }
    if (blacklist.some(r => url.match(r))) {
        console.log('blacklisted url', url);
        return;
    }

    if (await isUrlAlreadyCrawled(url)) {
        console.log('already crawled', url);
        return;
    }
    const article = await getArticle(url);
    if (!article) {
        return;
    }
    const links = getArticleLinks(article);
    await publishLinks(links, url);
    await saveArticle(url, article, links);
};

async function isUrlAlreadyCrawled(url) {
    try {
        const key = datastore.key([process.env.DATASTORE_KIND, url]);
        const article = await datastore.get(key);
        return Boolean(article[0]);
    } catch (e) {
        console.error('Could not get url from datastore to check', e.message);
        return false;
    }
}

/**
 * @param {String} url
 * @returns {Promise<boolean|Object>}
 */
async function getArticle(url) {
    try {
        const response = await axios.get(url);
        if (!response.headers['content-type'] || !response.headers['content-type'].includes('html')) {
            return false;
        }
        const doc = new JSDOM(response.data, {url});
        if (!isProbablyReaderable(doc.window.document)) {
            return false;
        }
        let reader = new Readability(doc.window.document);
        return reader.parse();
    } catch (e) {
        return false;
    }
}

/**
 * @param {Object} article
 * @returns {String[]}
 */
function getArticleLinks(article) {
    const articleDoc = new JSDOM(article.content);
    const links = [];
    for (let link of articleDoc.window.document.querySelectorAll('a')) {
        links.push(link.href.split('?')[0]);
    }
    return links.filter(isLinkStrictUrl);
}

/**
 * @param {String} urlString
 * @returns {boolean}
 */
function isLinkStrictUrl(urlString) {
    try {
        const url = new URL(urlString);
        return !(!url.hostname && url.hostname === 'localhost');
    } catch (e) {
        return false;
    }
}

/**
 * @param {String} url
 * @returns {boolean}
 */
function isLinkWhiteListed(url) {
    return whitelist.some(r => Boolean(url.match(r)));
}

/**
 * @param links
 * @param parentUrl
 * @returns {Promise<string[]>}
 */
async function publishLinks(links, parentUrl) {
    return Promise.all(links.map(link => newLinkPublisher.publish(Buffer.from(link), { parentUrl })))
}

/**
 * @param {String} url
 * @param {Object} article
 * @param {String[]} links
 * @returns {Promise<void>}
 */
async function saveArticle(url, article, links) {
    try {
        await datastore.save({
            key: datastore.key([process.env.DATASTORE_KIND, url]),
            data: {
                crawledAt: Date.now()
            },
        });
        await newArticlePublisher.publish(Buffer.from(JSON.stringify({url, article, links})))
    } catch (e) {
        console.error('save error', e.message);
    }
}
