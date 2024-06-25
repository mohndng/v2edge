// @ts-ignore
import { connect } from 'cloudflare:sockets';

// 建议修改为自己的 UUID
let userID = '8820e16b-fbc2-49d3-90e4-eeeb8301c83c';

// 生成配置文件的 Cloudflare 优选 IP (www.gov.se   speed.cloudflare.com等使用Cloudflare CDN的网站也可以)
const bestCFIP = "www.globe.com.ph"

// 用于 Cloudflare 网站的代理 IP
const proxyIPs = ["45.32.25.171"]; // （https://github.com/HappyLeslieAlexander/Cloudflare_VLESS/blob/main/proxyip.txt 中的地址也可以）
let proxyIP = proxyIPs[Math.floor(Math.random() * proxyIPs.length)];

let dohURL = 'https://cloudflare-dns.com/dns-query'; // https://cloudflare-dns.com/dns-query or https://dns.google/dns-query

// v2board api environment variables
let nodeId = ''; // 1

let apiToken = ''; //abcdefghijklmnopqrstuvwxyz123456

let apiHost = ''; // api.v2board.com

if (!isValidUUID(userID)) {
    throw new Error('uuid is not valid');
}

export default {
    /**
     * @param {import("@cloudflare/workers-types").Request} request
     * @param {{UID: string, PROXYIP: string, DNS_RESOLVER_URL: string, NODE_ID: int, API_HOST: string, API_TOKEN: string}} env
     * @param {import("@cloudflare/workers-types").ExecutionContext} ctx
     * @returns {Promise<Response>}
     */
    async fetch(request, env, ctx) {
        try {
            userID = env.UID || userID;
            proxyIP = env.PROXYIP || proxyIP;
            dohURL = env.DNS_RESOLVER_URL || dohURL;
            nodeId = env.NODE_ID || nodeId;
            apiToken = env.API_TOKEN || apiToken;
            apiHost = env.API_HOST || apiHost;
            const upgradeHeader = request.headers.get('Upgrade');
            if (!upgradeHeader || upgradeHeader !== 'websocket') {
                const url = new URL(request.url);
                switch (url.pathname) {
                    case '/cf':
                        return new Response(JSON.stringify(request.cf, null, 4), {
                            status: 200,
                            headers: {
                                "Content-Type": "application/json;charset=utf-8",
                            },
                        });
                    case '/connect': // for test connect to Cloudflare socket
                        const [hostname, port] = ['cloudflare.com', '80'];
                        console.log(`Connecting to ${hostname}:${port}...`);

                        try {
                            const socket = await connect({
                                hostname: hostname,
                                port: parseInt(port, 10),
                            });

                            const writer = socket.writable.getWriter();

                            try {
                                await writer.write(new TextEncoder().encode('GET / HTTP/1.1\r\nHost: ' + hostname + '\r\n\r\n'));
                            } catch (writeError) {
                                writer.releaseLock();
                                await socket.close();
                                return new Response(writeError.message, { status: 500 });
                            }

                            writer.releaseLock();

                            const reader = socket.readable.getReader();
                            let value;

                            try {
                                const result = await reader.read();
                                value = result.value;
                            } catch (readError) {
                                await reader.releaseLock();
                                await socket.close();
                                return new Response(readError.message, { status: 500 });
                            }

                            await reader.releaseLock();
                            await socket.close();

                            return new Response(new TextDecoder().decode(value), { status: 200 });
                        } catch (connectError) {
                            return new Response(connectError.message, { status: 500 });
                        }
                    case `/${userID}`: {
                        const vlessConfig = getVLESSConfig(userID, request.headers.get('Host'));
                        return new Response(`${vlessConfig}`, {
                            status: 200,
                            headers: {
                                "Content-Type": "text/plain;charset=utf-8",
                            }
                        });
                    }
                    case `/${userID}/base64`: {
                        const base64Config = getBase64Config(userID, request.headers.get('Host'));
                        return new Response(`${base64Config}`, {
                            status: 200,
                            headers: {
                                "Content-Type": "text/plain;charset=utf-8",
                            }
                        });
                    }
                    case `/${userID}/clash`: {
                        const clashConfig = getClashConfig(userID, request.headers.get('Host'));
                        return new Response(`${clashConfig}`, {
                            status: 200,
                            headers: {
                                "Content-Type": "text/plain;charset=utf-8",
                            }
                        });
                    }
                    case `/${userID}/sb`: {
                        const singConfig = getSingConfig(userID, request.headers.get('Host'));
                        return new Response(`${singConfig}`, {
                            status: 200,
                            headers: {
                                "Content-Type": "application/json;charset=utf-8",
                            }
                        });
                    }
                    default:
                        // return new Response('Not found', { status: 404 });
                        // For any other path, reverse proxy to 'leslieblog.top' and return the original response
                        url.hostname = 'www.bing.com';
                        url.protocol = 'https:';
                        request = new Request(url, request);
                        return await fetch(request);
                }
            } else {
                return await vlessOverWSHandler(request);
            }
        } catch (err) {
			/** @type {Error} */ let e = err;
            return new Response(e.toString());
        }
    },
};




/**
 * 
 * @param {import("@cloudflare/workers-types").Request} request
 */
async function vlessOverWSHandler(request) {

    /** @type {import("@cloudflare/workers-types").WebSocket[]} */
    // @ts-ignore
    const webSocketPair = new WebSocketPair();
    const [client, webSocket] = Object.values(webSocketPair);

    webSocket.accept();

    let address = '';
    let portWithRandomLog = '';
    const log = (/** @type {string} */ info, /** @type {string | undefined} */ event) => {
        console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
    };
    const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

    const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

    /** @type {{ value: import("@cloudflare/workers-types").Socket | null}}*/
    let remoteSocketWapper = {
        value: null,
    };
    let udpStreamWrite = null;
    let isDns = false;

    // ws --> remote
    readableWebSocketStream.pipeTo(new WritableStream({
        async write(chunk, controller) {
            if (isDns && udpStreamWrite) {
                return udpStreamWrite(chunk);
            }
            if (remoteSocketWapper.value) {
                const writer = remoteSocketWapper.value.writable.getWriter()
                await writer.write(chunk);
                writer.releaseLock();
                return;
            }

            const {
                hasError,
                message,
                portRemote = 443,
                addressRemote = '',
                rawDataIndex,
                vlessVersion = new Uint8Array([0, 0]),
                isUDP,
            } = await processVlessHeader(chunk, userID);
            address = addressRemote;
            portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '
                } `;
            if (hasError) {
                // controller.error(message);
                throw new Error(message); // Cloudflare seems has bug, controller.error will not end stream
                // webSocket.close(1000, message);
                return;
            }
            // if UDP but port not DNS port, close it
            if (isUDP) {
                if (portRemote === 53) {
                    isDns = true;
                } else {
                    // controller.error('UDP proxy only enable for DNS which is port 53');
                    throw new Error('UDP proxy only enable for DNS which is port 53'); // Cloudflare seems has bug, controller.error will not end stream
                    return;
                }
            }
            // ["version", "附加信息长度 N"]
            const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
            const rawClientData = chunk.slice(rawDataIndex);

            // TODO: support udp here when Cloudflare runtime has udp support
            if (isDns) {
                const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader, log);
                udpStreamWrite = write;
                udpStreamWrite(rawClientData);
                return;
            }
            handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log);
        },
        close() {
            log(`readableWebSocketStream is close`);
        },
        abort(reason) {
            log(`readableWebSocketStream is abort`, JSON.stringify(reason));
        },
    })).catch((err) => {
        log('readableWebSocketStream pipeTo error', err);
    });

    return new Response(null, {
        status: 101,
        // @ts-ignore
        webSocket: client,
    });
}

let apiResponseCache = null;
let cacheTimeout = null;

/**
 * Fetches the API response from the server and caches it for future use.
 * @returns {Promise<object|null>} A Promise that resolves to the API response object or null if there was an error.
 */
async function fetchApiResponse() {
    const requestOptions = {
        method: 'GET',
        redirect: 'follow'
    };

    try {
        const response = await fetch(`https://${apiHost}/api/v1/server/UniProxy/user?node_id=${nodeId}&node_type=v2ray&token=${apiToken}`, requestOptions);

        if (!response.ok) {
            console.error('Error: Network response was not ok');
            return null;
        }
        const apiResponse = await response.json();
        apiResponseCache = apiResponse;

        // Refresh the cache every 5 minutes (300000 milliseconds)
        if (cacheTimeout) {
            clearTimeout(cacheTimeout);
        }
        cacheTimeout = setTimeout(() => fetchApiResponse(), 300000);

        return apiResponse;
    } catch (error) {
        console.error('Error:', error);
        return null;
    }
}

/**
 * Returns the cached API response if it exists, otherwise fetches the API response from the server and caches it for future use.
 * @returns {Promise<object|null>} A Promise that resolves to the cached API response object or the fetched API response object, or null if there was an error.
 */
async function getApiResponse() {
    if (!apiResponseCache) {
        return await fetchApiResponse();
    }
    return apiResponseCache;
}

/**
 * Checks if a given UUID is present in the API response.
 * @param {string} targetUuid The UUID to search for.
 * @returns {Promise<boolean>} A Promise that resolves to true if the UUID is present in the API response, false otherwise.
 */
async function checkUuidInApiResponse(targetUuid) {
    // Check if any of the environment variables are empty
    if (!nodeId || !apiToken || !apiHost) {
        return false;
    }

    try {
        const apiResponse = await getApiResponse();
        if (!apiResponse) {
            return false;
        }
        const isUuidInResponse = apiResponse.users.some(user => user.uuid === targetUuid);
        return isUuidInResponse;
    } catch (error) {
        console.error('Error:', error);
        return false;
    }
}

// Usage example:
//   const targetUuid = "65590e04-a94c-4c59-a1f2-571bce925aad";
//   checkUuidInApiResponse(targetUuid).then(result => console.log(result));

/**
 * Handles outbound TCP connections.
 *
 * @param {any} remoteSocket 
 * @param {string} addressRemote The remote address to connect to.
 * @param {number} portRemote The remote port to connect to.
 * @param {Uint8Array} rawClientData The raw client data to write.
 * @param {import("@cloudflare/workers-types").WebSocket} webSocket The WebSocket to pass the remote socket to.
 * @param {Uint8Array} vlessResponseHeader The VLESS response header.
 * @param {function} log The logging function.
 * @returns {Promise<void>} The remote socket.
 */
async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log,) {
    async function connectAndWrite(address, port) {
        /** @type {import("@cloudflare/workers-types").Socket} */
        const tcpSocket = connect({
            hostname: address,
            port: port,
        });
        remoteSocket.value = tcpSocket;
        log(`connected to ${address}:${port}`);
        const writer = tcpSocket.writable.getWriter();
        await writer.write(rawClientData); // first write, nomal is tls client hello
        writer.releaseLock();
        return tcpSocket;
    }

    // if the Cloudflare connect tcp socket have no incoming data, we retry to redirect ip
    async function retry() {
        const tcpSocket = await connectAndWrite(proxyIP || addressRemote, portRemote)
        // no matter retry success or not, close websocket
        tcpSocket.closed.catch(error => {
            console.log('retry tcpSocket closed error', error);
        }).finally(() => {
            safeCloseWebSocket(webSocket);
        })
        remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
    }

    const tcpSocket = await connectAndWrite(addressRemote, portRemote);

    // when remoteSocket is ready, pass to websocket
    // remote--> ws
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry, log);
}

/**
 * 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocketServer
 * @param {string} earlyDataHeader for ws 0rtt
 * @param {(info: string)=> void} log for ws 0rtt
 */
function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
    let readableStreamCancel = false;
    const stream = new ReadableStream({
        start(controller) {
            webSocketServer.addEventListener('message', (event) => {
                if (readableStreamCancel) {
                    return;
                }
                const message = event.data;
                controller.enqueue(message);
            });

            // The event means that the client closed the client -> server stream.
            // However, the server -> client stream is still open until you call close() on the server side.
            // The WebSocket protocol says that a separate close message must be sent in each direction to fully close the socket.
            webSocketServer.addEventListener('close', () => {
                // client send close, need close server
                // if stream is cancel, skip controller.close
                safeCloseWebSocket(webSocketServer);
                if (readableStreamCancel) {
                    return;
                }
                controller.close();
            }
            );
            webSocketServer.addEventListener('error', (err) => {
                log('webSocketServer has error');
                controller.error(err);
            }
            );
            // for ws 0rtt
            const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
            if (error) {
                controller.error(error);
            } else if (earlyData) {
                controller.enqueue(earlyData);
            }
        },

        pull(controller) {
            // if ws can stop read if stream is full, we can implement backpressure
            // https://streams.spec.whatwg.org/#example-rs-push-backpressure
        },
        cancel(reason) {
            // 1. pipe WritableStream has error, this cancel will called, so ws handle server close into here
            // 2. if readableStream is cancel, all controller.close/enqueue need skip,
            // 3. but from testing controller.error still work even if readableStream is cancel
            if (readableStreamCancel) {
                return;
            }
            log(`ReadableStream was canceled, due to ${reason}`)
            readableStreamCancel = true;
            safeCloseWebSocket(webSocketServer);
        }
    });

    return stream;

}

// https://xtls.github.io/development/protocols/vless.html
// https://github.com/zizifn/excalidraw-backup/blob/main/v2ray-protocol.excalidraw

/**
 * 
 * @param { ArrayBuffer} vlessBuffer 
 * @param {string} userID 
 * @returns 
 */
async function processVlessHeader(
    vlessBuffer,
    userID
) {
    if (vlessBuffer.byteLength < 24) {
        return {
            hasError: true,
            message: 'invalid data',
        };
    }
    const version = new Uint8Array(vlessBuffer.slice(0, 1));
    let isValidUser = false;
    let isUDP = false;
    const slicedBuffer = new Uint8Array(vlessBuffer.slice(1, 17));
    const slicedBufferString = stringify(slicedBuffer);

    const uuids = userID.includes(',') ? userID.split(",") : [userID];

    const checkUuidInApi = await checkUuidInApiResponse(slicedBufferString);
    isValidUser = uuids.some(userUuid => checkUuidInApi || slicedBufferString === userUuid.trim());

    console.log(`checkUuidInApi: ${await checkUuidInApiResponse(slicedBufferString)}, userID: ${slicedBufferString}`);

    if (!isValidUser) {
        return {
            hasError: true,
            message: 'invalid user',
        };
    }

    const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
    //skip opt for now

    const command = new Uint8Array(
        vlessBuffer.slice(18 + optLength, 18 + optLength + 1)
    )[0];

    // 0x01 TCP
    // 0x02 UDP
    // 0x03 MUX
    if (command === 1) {
    } else if (command === 2) {
        isUDP = true;
    } else {
        return {
            hasError: true,
            message: `command ${command} is not support, command 01-tcp,02-udp,03-mux`,
        };
    }
    const portIndex = 18 + optLength + 1;
    const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
    // port is big-Endian in raw data etc 80 == 0x005d
    const portRemote = new DataView(portBuffer).getUint16(0);

    let addressIndex = portIndex + 2;
    const addressBuffer = new Uint8Array(
        vlessBuffer.slice(addressIndex, addressIndex + 1)
    );

    // 1--> ipv4  addressLength =4
    // 2--> domain name addressLength=addressBuffer[1]
    // 3--> ipv6  addressLength =16
    const addressType = addressBuffer[0];
    let addressLength = 0;
    let addressValueIndex = addressIndex + 1;
    let addressValue = '';
    switch (addressType) {
        case 1:
            addressLength = 4;
            addressValue = new Uint8Array(
                vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
            ).join('.');
            break;
        case 2:
            addressLength = new Uint8Array(
                vlessBuffer.slice(addressValueIndex, addressValueIndex + 1)
            )[0];
            addressValueIndex += 1;
            addressValue = new TextDecoder().decode(
                vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
            );
            break;
        case 3:
            addressLength = 16;
            const dataView = new DataView(
                vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
            );
            // 2001:0db8:85a3:0000:0000:8a2e:0370:7334
            const ipv6 = [];
            for (let i = 0; i < 8; i++) {
                ipv6.push(dataView.getUint16(i * 2).toString(16));
            }
            addressValue = ipv6.join(':');
            // seems no need add [] for ipv6
            break;
        default:
            return {
                hasError: true,
                message: `invild  addressType is ${addressType}`,
            };
    }
    if (!addressValue) {
        return {
            hasError: true,
            message: `addressValue is empty, addressType is ${addressType}`,
        };
    }

    return {
        hasError: false,
        addressRemote: addressValue,
        addressType,
        portRemote,
        rawDataIndex: addressValueIndex + addressLength,
        vlessVersion: version,
        isUDP,
    };
}


/**
 * 
 * @param {import("@cloudflare/workers-types").Socket} remoteSocket 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocket 
 * @param {ArrayBuffer} vlessResponseHeader 
 * @param {(() => Promise<void>) | null} retry
 * @param {*} log 
 */
async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
    // remote--> ws
    let remoteChunkCount = 0;
    let chunks = [];
    /** @type {ArrayBuffer | null} */
    let vlessHeader = vlessResponseHeader;
    let hasIncomingData = false; // check if remoteSocket has incoming data
    await remoteSocket.readable
        .pipeTo(
            new WritableStream({
                start() {
                },
                /**
                 * 
                 * @param {Uint8Array} chunk 
                 * @param {*} controller 
                 */
                async write(chunk, controller) {
                    hasIncomingData = true;
                    // remoteChunkCount++;
                    if (webSocket.readyState !== WS_READY_STATE_OPEN) {
                        controller.error(
                            'webSocket.readyState is not open, maybe close'
                        );
                    }
                    if (vlessHeader) {
                        webSocket.send(await new Blob([vlessHeader, chunk]).arrayBuffer());
                        vlessHeader = null;
                    } else {
                        // seems no need rate limit this, Cloudflare seems fix this??..
                        // if (remoteChunkCount > 20000) {
                        // 	// Cloudflare one package is 4096 byte(4kb),  4096 * 20000 = 80M
                        // 	await delay(1);
                        // }
                        webSocket.send(chunk);
                    }
                },
                close() {
                    log(`remoteConnection!.readable is close with hasIncomingData is ${hasIncomingData}`);
                    // safeCloseWebSocket(webSocket); // no need server close websocket frist for some case will casue HTTP ERR_CONTENT_LENGTH_MISMATCH issue, client will send close event anyway.
                },
                abort(reason) {
                    console.error(`remoteConnection!.readable abort`, reason);
                },
            })
        )
        .catch((error) => {
            console.error(
                `remoteSocketToWS has exception `,
                error.stack || error
            );
            safeCloseWebSocket(webSocket);
        });

    // seems is Cloudflare connect socket have error,
    // 1. Socket.closed will have error
    // 2. Socket.readable will be close without any data coming
    if (hasIncomingData === false && retry) {
        log(`retry`)
        retry();
    }
}

/**
 * 
 * @param {string} base64Str 
 * @returns 
 */
function base64ToArrayBuffer(base64Str) {
    if (!base64Str) {
        return { error: null };
    }
    try {
        // go use modified Base64 for URL rfc4648 which js atob not support
        base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
        const decode = atob(base64Str);
        const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
        return { earlyData: arryBuffer.buffer, error: null };
    } catch (error) {
        return { error };
    }
}

/**
 * This is not real UUID validation
 * @param {string} uuid 
 */
function isValidUUID(uuid) {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuidRegex.test(uuid);
}

const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;
/**
 * Normally, WebSocket will not has exceptions when close.
 * @param {import("@cloudflare/workers-types").WebSocket} socket
 */
function safeCloseWebSocket(socket) {
    try {
        if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
            socket.close();
        }
    } catch (error) {
        console.error('safeCloseWebSocket error', error);
    }
}

const byteToHex = [];
for (let i = 0; i < 256; ++i) {
    byteToHex.push((i + 256).toString(16).slice(1));
}
function unsafeStringify(arr, offset = 0) {
    return (byteToHex[arr[offset + 0]] + byteToHex[arr[offset + 1]] + byteToHex[arr[offset + 2]] + byteToHex[arr[offset + 3]] + "-" + byteToHex[arr[offset + 4]] + byteToHex[arr[offset + 5]] + "-" + byteToHex[arr[offset + 6]] + byteToHex[arr[offset + 7]] + "-" + byteToHex[arr[offset + 8]] + byteToHex[arr[offset + 9]] + "-" + byteToHex[arr[offset + 10]] + byteToHex[arr[offset + 11]] + byteToHex[arr[offset + 12]] + byteToHex[arr[offset + 13]] + byteToHex[arr[offset + 14]] + byteToHex[arr[offset + 15]]).toLowerCase();
}
function stringify(arr, offset = 0) {
    const uuid = unsafeStringify(arr, offset);
    if (!isValidUUID(uuid)) {
        throw TypeError("Stringified UUID is invalid");
    }
    return uuid;
}


/**
 * 
 * @param {import("@cloudflare/workers-types").WebSocket} webSocket 
 * @param {ArrayBuffer} vlessResponseHeader 
 * @param {(string)=> void} log 
 */
async function handleUDPOutBound(webSocket, vlessResponseHeader, log) {

    let isVlessHeaderSent = false;
    const transformStream = new TransformStream({
        start(controller) {

        },
        transform(chunk, controller) {
            // udp message 2 byte is the the length of udp data
            // TODO: this should have bug, beacsue maybe udp chunk can be in two websocket message
            for (let index = 0; index < chunk.byteLength;) {
                const lengthBuffer = chunk.slice(index, index + 2);
                const udpPakcetLength = new DataView(lengthBuffer).getUint16(0);
                const udpData = new Uint8Array(
                    chunk.slice(index + 2, index + 2 + udpPakcetLength)
                );
                index = index + 2 + udpPakcetLength;
                controller.enqueue(udpData);
            }
        },
        flush(controller) {
        }
    });

    // only handle dns udp for now
    transformStream.readable.pipeTo(new WritableStream({
        async write(chunk) {
            const resp = await fetch(dohURL, // dns server url
                {
                    method: 'POST',
                    headers: {
                        'content-type': 'application/dns-message',
                    },
                    body: chunk,
                })
            const dnsQueryResult = await resp.arrayBuffer();
            const udpSize = dnsQueryResult.byteLength;
            // console.log([...new Uint8Array(dnsQueryResult)].map((x) => x.toString(16)));
            const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
            if (webSocket.readyState === WS_READY_STATE_OPEN) {
                log(`doh success and dns message length is ${udpSize}`);
                if (isVlessHeaderSent) {
                    webSocket.send(await new Blob([udpSizeBuffer, dnsQueryResult]).arrayBuffer());
                } else {
                    webSocket.send(await new Blob([vlessResponseHeader, udpSizeBuffer, dnsQueryResult]).arrayBuffer());
                    isVlessHeaderSent = true;
                }
            }
        }
    })).catch((error) => {
        log('dns udp has error' + error)
    });

    const writer = transformStream.writable.getWriter();

    return {
        /**
         * 
         * @param {Uint8Array} chunk 
         */
        write(chunk) {
            writer.write(chunk);
        }
    };
}

/**
 * 
 * @param {string} userID 
 * @param {string | null} hostName
 * @returns {string}
 */
function getVLESSConfig(userID, hostName) {
    const vlessLink = `vless://${userID}\u0040${bestCFIP}:80?encryption=none&security=none&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Workers/Pages`
    const vlessTlsLink = `vless://${userID}\u0040${bestCFIP}:443?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2Fneufcent#NeufCent`
    return `

𝘽𝙚𝙡𝙤𝙬 𝙞𝙨 𝙩𝙝𝙚 𝙣𝙤𝙙𝙚 𝙞𝙣𝙛𝙤𝙧𝙢𝙖𝙩𝙞𝙤𝙣 𝙖𝙣𝙙 𝙣𝙤𝙙𝙚 𝙨𝙝𝙖𝙧𝙞𝙣𝙜 𝙡𝙞𝙣𝙠 𝙛𝙤𝙧 𝙣𝙤𝙣-𝙏𝙇𝙎 𝙥𝙤𝙧𝙩𝙨. 𝙔𝙤𝙪 𝙘𝙖𝙣 𝙪𝙨𝙚 𝙩𝙝𝙚 𝙣𝙤𝙣-𝙏𝙇𝙎 𝙥𝙤𝙧𝙩𝙨 𝙨𝙪𝙥𝙥𝙤𝙧𝙩𝙚𝙙 𝙗𝙮 𝘾𝙡𝙤𝙪𝙙𝙛𝙡𝙖𝙧𝙚:

Address：${hostName} 
Port：443 
UUID：${userID}
Protocol：ws
Transport Layer Security：TLS
CNAME：${hostName}
Path：/?ed=2048
SNI Domain：${hostName}

${vlessTlsLink}


Note: In some areas, Cloudflare's default domain name is polluted. Unless you turn on the TLS fragmentation function on the client, you cannot use the node of the TLS port. If the node is deployed for Pages, only nodes with TLS ports can be used.

---------------------------------------------------------------

---------------------------------------------------------------

█▀▄▀█ █▀█ █▄░█ ▄▀█ █▀█ █▀▀ █░█
█░▀░█ █▄█ █░▀█ █▀█ █▀▄ █▄▄ █▀█
`;
}

function getBase64Config(userID, hostName) {
    const vlessLinks = btoa(`vless://${userID}\u0040${bestCFIP}:80?encryption=none&security=none&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-80\nvless://${userID}\u0040${bestCFIP}:8080?encryption=none&security=none&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-8080\nvless://${userID}\u0040${bestCFIP}:8880?encryption=none&security=none&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-8880\nvless://${userID}\u0040${bestCFIP}:2052?encryption=none&security=none&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-2052\nvless://${userID}\u0040${bestCFIP}:2082?encryption=none&security=none&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-2082\nvless://${userID}\u0040${bestCFIP}:2086?encryption=none&security=none&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-2086\nvless://${userID}\u0040${bestCFIP}:2095?encryption=none&security=none&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-2095\nvless://${userID}\u0040${bestCFIP}:443?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-TLS-443\nvless://${userID}\u0040${bestCFIP}:2053?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-TLS-2053\nvless://${userID}\u0040${bestCFIP}:2083?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-TLS-2083\nvless://${userID}\u0040${bestCFIP}:2087?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-TLS-2087\nvless://${userID}\u0040${bestCFIP}:2096?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-TLS-2096\nvless://${userID}\u0040${bestCFIP}:8443?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#Leslie-Cloudflare-vless-TLS-8443`);

    return `${vlessLinks}`
}

function getClashConfig(userID, hostName) {
    return `port: 7890
allow-lan: true
mode: rule
log-level: info
unified-delay: true
global-client-fingerprint: chrome
dns:
  enable: true
  listen: :53
  ipv6: true
  enhanced-mode: fake-ip
  fake-ip-range: 198.18.0.1/16
  default-nameserver: 
    - 223.5.5.5
    - 1.1.1.1
  nameserver:
    - https://dns.alidns.com/dns-query
    - https://cloudflare-dns.com/dns-query
  fallback:
    - https://cloudflare-dns.com/dns-query
  fallback-filter:
    geoip: true
    geoip-code: CN
    ipcidr:
      - 240.0.0.0/4

proxies:
- name: Cloudflare-vless-80
  type: vless
  server: ${bestCFIP}
  port: 80
  uuid: ${userID}
  udp: false
  tls: false
  network: ws
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-8080
  type: vless
  server: ${bestCFIP}
  port: 8080
  uuid: ${userID}
  udp: false
  tls: false
  network: ws
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-8880
  type: vless
  server: ${bestCFIP}
  port: 8880
  uuid: ${userID}
  udp: false
  tls: false
  network: ws
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-2052
  type: vless
  server: ${bestCFIP}
  port: 2052
  uuid: ${userID}
  udp: false
  tls: false
  network: ws
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-2082
  type: vless
  server: ${bestCFIP}
  port: 2082
  uuid: ${userID}
  udp: false
  tls: false
  network: ws
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-2086
  type: vless
  server: ${bestCFIP}
  port: 2086
  uuid: ${userID}
  udp: false
  tls: false
  network: ws
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-2095
  type: vless
  server: ${bestCFIP}
  port: 2095
  uuid: ${userID}
  udp: false
  tls: false
  network: ws
  servername: ${hostName}
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-tls-443
  type: vless
  server: ${bestCFIP}
  port: 443
  uuid: ${userID}
  udp: false
  tls: true
  network: ws
  servername: ${hostName}
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-tls-2053
  type: vless
  server: ${bestCFIP}
  port: 2053
  uuid: ${userID}
  udp: false
  tls: true
  network: ws
  servername: ${hostName}
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-tls-2083
  type: vless
  server: ${bestCFIP}
  port: 2083
  uuid: ${userID}
  udp: false
  tls: true
  network: ws
  servername: ${hostName}
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-tls-2087
  type: vless
  server: ${bestCFIP}
  port: 2087
  uuid: ${userID}
  udp: false
  tls: true
  network: ws
  servername: ${hostName}
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-tls-2096
  type: vless
  server: ${bestCFIP}
  port: 2096
  uuid: ${userID}
  udp: false
  tls: true
  network: ws
  servername: ${hostName}
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

- name: Cloudflare-vless-tls-8443
  type: vless
  server: ${bestCFIP}
  port: 8443
  uuid: ${userID}
  udp: false
  tls: true
  network: ws
  servername: ${hostName}
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}

proxy-groups:
- name: 负载均衡
  type: load-balance
  url: http://www.gstatic.com/generate_204
  interval: 300
  proxies:
    - Cloudflare-vless-80
    - Cloudflare-vless-8080
    - Cloudflare-vless-8880
    - Cloudflare-vless-2052
    - Cloudflare-vless-2082
    - Cloudflare-vless-2086
    - Cloudflare-vless-2095
    - Cloudflare-vless-tls-443
    - Cloudflare-vless-tls-2053
    - Cloudflare-vless-tls-2083
    - Cloudflare-vless-tls-2087
    - Cloudflare-vless-tls-2096
    - Cloudflare-vless-tls-8443

- name: 自动选择
  type: url-test
  url: http://www.gstatic.com/generate_204
  interval: 300
  tolerance: 50
  proxies:
    - Cloudflare-vless-80
    - Cloudflare-vless-8080
    - Cloudflare-vless-8880
    - Cloudflare-vless-2052
    - Cloudflare-vless-2082
    - Cloudflare-vless-2086
    - Cloudflare-vless-2095
    - Cloudflare-vless-tls-443
    - Cloudflare-vless-tls-2053
    - Cloudflare-vless-tls-2083
    - Cloudflare-vless-tls-2087
    - Cloudflare-vless-tls-2096
    - Cloudflare-vless-tls-8443
    
- name: 🌍选择代理
  type: select
  proxies:
    - 负载均衡
    - 自动选择
    - DIRECT
    - Cloudflare-vless-80
    - Cloudflare-vless-8080
    - Cloudflare-vless-8880
    - Cloudflare-vless-2052
    - Cloudflare-vless-2082
    - Cloudflare-vless-2086
    - Cloudflare-vless-2095
    - Cloudflare-vless-tls-443
    - Cloudflare-vless-tls-2053
    - Cloudflare-vless-tls-2083
    - Cloudflare-vless-tls-2087
    - Cloudflare-vless-tls-2096
    - Cloudflare-vless-tls-8443

rules:
  - GEOIP,LAN,DIRECT
  - GEOIP,CN,DIRECT
  - MATCH,🌍选择代理`
}

function getSingConfig(userID, hostName) {
    return `{
  "log": {
    "disabled": false,
    "level": "info",
    "timestamp": true
  },
  "experimental": {
    "clash_api": {
      "external_controller": "127.0.0.1:9090",
      "external_ui": "ui",
      "external_ui_download_url": "",
      "external_ui_download_detour": "",
      "secret": "",
      "default_mode": "Rule"
    },
    "cache_file": {
      "enabled": true,
      "path": "cache.db",
      "store_fakeip": true
    }
  },
    "dns": {
        "fakeip": {
            "enabled": true,
            "inet4_range": "198.18.0.0/15",
            "inet6_range": "fc00::/18"
        },
        "independent_cache": true,
        "rules": [
            {
                "outbound": [
                    "any"
                ],
                "server": "local"
            },
            {
                "query_type": [
                    "A",
                    "AAAA"
                ],
                "rewrite_ttl": 1,
                "server": "fakeip"
            },
            {
                "clash_mode": "global",
                "server": "remote"
            },
            {
                "clash_mode": "direct",
                "server": "local"
            },
            {
                "rule_set": "geosite-cn",
                "server": "local"
            }
        ],
        "servers": [
            {
                "address": "https://1.1.1.1/dns-query",
                "detour": "select",
                "tag": "remote"
            },
            {
                "address": "https://223.5.5.5/dns-query",
                "detour": "direct",
                "tag": "local"
            },
            {
                "address": "rcode://success",
                "tag": "block"
            },
            {
                "address": "fakeip",
                "tag": "fakeip"
            }
        ],
        "strategy": "prefer_ipv4"
    },
    "experimental": {
        "cache_file": {
            "enabled": true
        },
        "clash_api": {
            "external_controller": "127.0.0.1:9090",
            "secret": ""
        }
    },
    "inbounds": [
        {
            "auto_route": true,
            "domain_strategy": "prefer_ipv4",
            "endpoint_independent_nat": true,
            "inet4_address": "172.19.0.1/30",
            "inet6_address": "2001:0470:f9da:fdfa::1/64",
            "mtu": 9000,
            "sniff": true,
            "strict_route": true,
            "type": "tun"
        },
        {
            "domain_strategy": "prefer_ipv4",
            "listen": "127.0.0.1",
            "listen_port": 2333,
            "tag": "socks-in",
            "type": "socks",
            "users": []
        },
        {
            "domain_strategy": "prefer_ipv4",
            "listen": "127.0.0.1",
            "listen_port": 2334,
            "tag": "mixed-in",
            "type": "mixed",
            "users": []
        }
    ],
    "log": {},
    "outbounds": [
        {
            "tag": "select",
            "type": "selector",
            "default": "urltest",
            "outbounds": [
                "urltest",
                "Cloudflare-vless-80",
                "Cloudflare-vless-8080",
                "Cloudflare-vless-8880",
                "Cloudflare-vless-2052",
                "Cloudflare-vless-2082",
                "Cloudflare-vless-2086",
                "Cloudflare-vless-2095",
                "Cloudflare-vless-tls-443",
                "Cloudflare-vless-tls-2053",
                "Cloudflare-vless-tls-2083",
                "Cloudflare-vless-tls-2087",
                "Cloudflare-vless-tls-2096",
                "Cloudflare-vless-tls-8443"
            ]
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 80,
            "tag": "Cloudflare-vless-80",
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 8080,
            "tag": "Cloudflare-vless-8080",
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 8880,
            "tag": "Cloudflare-vless-8880",
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 2052,
            "tag": "Cloudflare-vless-2052",
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 2082,
            "tag": "Cloudflare-vless-2082",
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 2086,
            "tag": "Cloudflare-vless-2086",
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 2095,
            "tag": "Cloudflare-vless-2095",
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 443,
            "tag": "Cloudflare-vless-tls-443",
            "tls": {
                "enabled": true,
                "server_name": "vless2.leslieblog.top"
            },
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 2053,
            "tag": "Cloudflare-vless-tls-2053",
            "tls": {
                "enabled": true,
                "server_name": "vless2.leslieblog.top"
            },
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 2083,
            "tag": "Cloudflare-vless-tls-2083",
            "tls": {
                "enabled": true,
                "server_name": "vless2.leslieblog.top"
            },
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 2087,
            "tag": "Cloudflare-vless-tls-2087",
            "tls": {
                "enabled": true,
                "server_name": "vless2.leslieblog.top"
            },
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 2096,
            "tag": "Cloudflare-vless-tls-2096",
            "tls": {
                "enabled": true,
                "server_name": "vless2.leslieblog.top"
            },
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "server": "www.visa.com.hk",
            "server_port": 8443,
            "tag": "Cloudflare-vless-tls-8443",
            "tls": {
                "enabled": true,
                "server_name": "vless2.leslieblog.top"
            },
            "transport": {
                "headers": {
                    "Host": [
                        "vless2.leslieblog.top"
                    ]
                },
                "path": "/?ed=2048",
                "type": "ws"
            },
            "type": "vless",
            "uuid": "8820e16b-fbc2-49d3-90e4-eeeb8301c83c",
            "packet_encoding": "xudp"
        },
        {
            "tag": "urltest",
            "type": "urltest",
            "outbounds": [
                "Cloudflare-vless-80",
                "Cloudflare-vless-8080",
                "Cloudflare-vless-8880",
                "Cloudflare-vless-2052",
                "Cloudflare-vless-2082",
                "Cloudflare-vless-2086",
                "Cloudflare-vless-2095",
                "Cloudflare-vless-tls-443",
                "Cloudflare-vless-tls-2053",
                "Cloudflare-vless-tls-2083",
                "Cloudflare-vless-tls-2087",
                "Cloudflare-vless-tls-2096",
                "Cloudflare-vless-tls-8443"
            ]
        },
        {
            "tag": "direct",
            "type": "direct"
        },
        {
            "tag": "block",
            "type": "block"
        },
        {
            "tag": "dns-out",
            "type": "dns"
        }
    ],
    "route": {
        "auto_detect_interface": true,
        "rule_set": [
            {
                "format": "binary",
                "tag": "geoip-cn",
                "type": "remote",
                "url": "https://raw.githubusercontent.com/SagerNet/sing-geoip/rule-set/geoip-cn.srs"
            },
            {
                "format": "binary",
                "tag": "geosite-cn",
                "type": "remote",
                "url": "https://raw.githubusercontent.com/xmdhs/sing-geosite/rule-set-Loyalsoldier/geosite-geolocation-cn.srs"
            }
        ],
        "rules": [
            {
                "outbound": "dns-out",
                "port": 53
            },
            {
                "clash_mode": "direct",
                "outbound": "direct"
            },
            {
                "clash_mode": "global",
                "outbound": "select"
            },
            {
                "ip_is_private": true,
                "outbound": "direct"
            },
            {
                "outbound": "direct",
                "rule_set": "geoip-cn"
            }
        ]
    }
}`;
}
