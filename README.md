# LDK Integration into Breez SDK

## Motivation
LDK is a comprehensive set of general-purpose libraries that enables the development of Lightning Network-enabled applications. By encapsulating the complexity of the Lightning Network, LDK provides a powerful and flexible interface for builders.
However, to achieve even basic functionality, developers must still create custom integration code and set up supporting services, such as a block source, rapid gossip sync, and VSS. Moreover, for a fully-fledged application, additional services like LSP, swap services, and fiat on-ramps may be required. This complexity makes it challenging to integrate the Lightning Network into non-Lightning-focused applications, effectively limiting its practical adoption.

## Description
The goal of this project is to integrate LDK into the Breez SDK with a specific focus on running a Lightning node as an edge node connected to one or more LSPs, which means that the node will not forward payments. This will enable us to make informed decisions and develop services tailored to this use case. In essence, the project aims to narrow the scope of LDK to an edge node while enhancing functionality by leveraging existing Breez infrastructure and services.

The deliverable will be a Rust library featuring a simplified interface that aligns with other Breez SDKs, ensuring consistency and logical similarity. Internally, the library will create and operate a Lightning node using LDK, ensuring trustless and private operations. All necessary services will be set up and preconfigured. Additionally, Breez services such as LSP, swaps, and fiat on-ramps will be integrated.
The deliverable includes all components necessary for the safe and secure operation of a Lightning node, such as key management and cloud-based node state backup (VSS).

## Potential Impact
This integration will lower the entry barrier for application developers, making it easier to enable Lightning payments in a trustless, non-custodial manner. It will open Lightning to a broader audience, including projects not specifically focused on Lightning but on their own products, thereby attracting more end users to this sovereign means of payment.

## Prototype

The current prototype version lives in the [`protype` branch](https://github.com/andrei-21/breez-sdk/tree/prototype).

---

# **Breez SDK - Native *(Greenlight Implementation)***

## **Overview**

## **What Is the Breez SDK?**

The Breez SDK provides developers with a end-to-end solution for integrating self-custodial Lightning payments into their apps and services. It eliminates the need for third parties, simplifies the complexities of Bitcoin and Lightning, and enables seamless onboarding for billions of users to the future of peer-to-peer payments.

To provide the best experience for their end-users, developers can choose between the following implementations:

- [Breez SDK - Native *(Greenlight Implementation)*](https://sdk-doc.breez.technology/)
- [Breez SDK - Nodeless *(Liquid Implementation)*](https://sdk-doc-liquid.breez.technology/)

**The Breez SDK is free for developers.** 

Learn more about the Breez SDK — download our one pager [here](https://drive.google.com/file/d/1TDspNJOvrX_lZUxipeBzitPIWXIdSsLy/view?usp=sharing).

## **What Is the Breez SDK - Native *(Greenlight Implementation)*?**

It's a cloud-based Lightning integration that offers a self-custodial, end-to-end solution for integrating Lightning payments, utilizing nodes-on-demand provided by Blockstream’s Greenlight, with built-in Lightning Service Providers (LSP), on-chain interoperability, and third-party fiat on-ramps. Using the SDK you'll able to:

- **Send payments** via various protocols such as: Bolt11, LNURL-Pay, Lightning address, BTC address
- **Receive payments** via various protocols such as: Bolt11, LNURL-Withdraw, LNURL-Pay, Lightning address, BTC address

**Key Features**

- [x]  On-chain interoperability
- [x]  Built-in LSP
- [x]  Integrated watchtower
- [x]  Complete LNURL functionality
- [x]  Multi-app support
- [x]  Multi-device support
- [x]  Real-time state backup
- [x]  Keys are only held by users
- [x]  Built-in fiat on-ramp
- [x]  Free open-source solution

## Getting Started

Head over to the [Breez SDK - Native *(Greenlight Implementation)* documentation](https://sdk-doc.breez.technology/) to start implementing Lightning into your app.

Note: You'll need an API key to use the *Greenlight* implementation. For more info, email us at contact@breez.technology.

## **API**

API documentation is [here](https://breez.github.io/breez-sdk-greenlight/breez_sdk_core/).

## **Command line**

[Breez sdk-cli](https://github.com/breez/breez-sdk-greenlight/tree/main/tools/sdk-cli) is a command line client that allows you to interact with and test the functionality of the Breez SDK.

## **Support**

Have a question for the team? Join our [Telegram channel](https://t.me/breezsdk) or email us at [contact@breez.technology](mailto:contact@breez.technology) 

## How Does Native *(Greenlight Implementation)* Work?

The Breez SDK - Native *(Greenlight implementation)* allows end-users to send and receive payments using the Breez SDK through several key components:

- **Signer**: The app integrating the Breez SDK runs a validating signer that interacts with the end-user node.
- **Node**: End-user nodes are hosted on Blockstream’s Greenlight cloud infrastructure. The SDK creates a node when an end-user needs to send or receive a payment via the Lightning Network. Each end-user has their own node.
- **Lightning Service Providers (LSP)**: Design partners use LSPs, operated by entities other than Breez, to facilitate channel connectivity. LSP nodes are connected to Breez’s routing nodes, which in turn connect to other nodes in the Lightning Network.
- **Submarine Swaps**: Submarine swaps and reverse submarine swaps are used for transactions involving BTC addresses (on-chain). When receiving funds, submarine swaps convert the BTC to the user node on the Lightning Network. When sending funds to BTC addresses, reverse submarine swaps convert Lightning Network funds back to BTC.

![Breez SDK - Greenlight](https://github.com/breez/breez-sdk-docs/raw/main/src/images/BreezSDK_Greenlight.png)

## **Build & Test**

The libs folder contains three sub-folders and is a structured as a cargo workspace:

- **sdk-common**: common SDK functionality for [Breez SDK - Native](https://github.com/breez/breez-sdk-greenlight) and [Breez SDK - Nodeless](https://github.com/breez/breez-sdk-liquid).
- **sdk-core**: the core SDK rust library.
- **sdk-bindings**: ffi bindings for Kotlin, Python, Swift, C#, and Go.
- **sdk-flutter**: a flutter plugin (includes ffi bindings for dart).
- **sdk-react-native**: a react-native plugin.

The tools folder contains a simple command line interface (sdk-cli) to the SDK.

See the instructions in each sub-project readme on how to build, test, and run.

## **Contributing**

Contributions are always welcome. Please read our [contribution guide](CONTRIBUTING.md) to get started.

## SDK Development Roadmap

- [x]  ‘On-the-fly’ channel creation
- [x]  Send/receive bolt11
- [x]  LNURL-Pay
- [x]  LNURL-Withdraw
- [x]  Backup/restore using mnemonics
- [x]  Send zero-amount invoices
- [x]  Fiat currencies
- [x]  Send spontaneous payments
- [x]  Send to a Lightning address
- [x]  Send to BIP 21
- [x]  cli inteface
- [x]  Swift bindings
- [x]  Kotlin bindings
- [x]  API key
- [x]  Receive via on-chain address
- [x]  React Native bindings
- [x]  LNURL-Auth
- [x]  Send to an on-chain address
- [x]  MoonPay fiat on-ramp
- [x]  C# bindings
- [x]  Python bindings
- [x]  Spend all funds
- [x]  Webhook for receiving payments
- [x]  Offline receive via notifications
- [ ]  Taproot swaps
- [ ]  Send/receive bolt12
- [ ]  Simplifed key management (cloud key backup)
- [ ]  WebAssembly support
- [ ]  Improve key share
- [ ]  Close channels to a predefined address
- [ ]  Make mempool.space dependency optional
- [ ]  Receive 0 amount invoice via LSP (unified QR)
- [ ]  Async payments
- [ ]  LDK support
- [ ]  Splicing
