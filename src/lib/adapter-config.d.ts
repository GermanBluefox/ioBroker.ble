// This file extends the AdapterConfig type from "@types/iobroker"
// Augment the globally declared type ioBroker.AdapterConfig
declare global {
	namespace ioBroker {
		interface AdapterConfig {
			services: string;
			server: string;
			/** Instance of the ble-gateway adapter to receive the advertisements from, e.g. "ble-gateway.0" */
			gateway: string;
			hciDevice: number;
			plugins: string;
			allowEmptyDevices: boolean;
			rssiThrottle: number;
		}
	}
}
// this is required so the above AdapterConfig is found by TypeScript / type checking
export {};
