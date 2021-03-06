"use strict";
const global_1 = require("../lib/global");
function parseData(raw) {
    if (raw.length === 1) {
        // single byte
        return raw[0];
    }
    else {
        // Output hex value
        return raw.toString("hex");
    }
}
const plugin = {
    name: "_default",
    description: "Handles all peripherals that are not handled by other plugins",
    // Just handle all services we receive already
    advertisedServices: [],
    isHandling: (p) => true,
    // No special context necessary. Return the peripheral, so it gets passed to the other methods.
    createContext: (peripheral) => peripheral,
    defineObjects: (peripheral) => {
        const deviceObject = {
            common: undefined,
            native: undefined,
        };
        const channelId = `services`;
        const channelObject = {
            id: channelId,
            common: {
                // common
                name: "Advertised services",
                role: "info",
            },
            native: undefined,
        };
        const stateObjects = [];
        if (peripheral.advertisement && peripheral.advertisement.serviceData) {
            for (const entry of peripheral.advertisement.serviceData) {
                const uuid = entry.uuid;
                const stateId = `${channelId}.${uuid}`;
                stateObjects.push({
                    id: stateId,
                    common: {
                        role: "value",
                        name: "Advertised service " + uuid,
                        desc: "",
                        type: "mixed",
                        read: true,
                        write: false,
                    },
                    native: undefined,
                });
            }
        }
        return {
            device: deviceObject,
            channels: [channelObject],
            states: stateObjects,
        };
    },
    getValues: (peripheral) => {
        const ret = {};
        if (peripheral.advertisement && peripheral.advertisement.serviceData) {
            for (const entry of peripheral.advertisement.serviceData) {
                const uuid = entry.uuid;
                const stateId = `services.${uuid}`;
                // remember the transmitted data
                ret[stateId] = parseData(entry.data);
                global_1.Global.log(`_default: ${peripheral.address} > got data ${ret[stateId]} for ${uuid}`, "debug");
            }
        }
        return ret;
    },
};
module.exports = plugin;
