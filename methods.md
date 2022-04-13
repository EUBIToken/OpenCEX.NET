# Notes

1. Client send requests to server as an URL-encoded JSON array, prefixed with `OpenCEX_request_body=`
2. Void requests methods return null value
3. Requests are sent to server in batch, which is executed in parallel
4. Maximum batch size = 10 requests
5. In case of error, server returns the first failing request
6. If all requests succeed, server returns array of returned results
7. Authentication is cookie-based
8. Captcha-protected request method have an extra `string captcha` argument
9. Arguments are passed as JSON dictionary, and the order doesn't matter
10. Example request: `[{"method": "login", "username": "example", "password": "12345", "captcha": "examplecaptchachallenge"}]`
11. Example success response: `{"status": "success", "returns": ["an example returned string", 1]}`
12. Example error response: `{"status": "error", "reason": "Unexpected internal server error (should not reach here)!"}`
13. Clients must use the POST request method
14. Clients must pass `Origin: https://exchange.polyeubitoken.com` header
15. Clients can send requests to any URL they want
16. `opencex-net-dev.herokuapp.com` and `opencex-net-prod.herokuapp.com` are the two servers clients can send requests to
17. Server sends CORS headers to clients
18. All SafeUint values are returned to clients as base-10 string
19. Clients pass SafeUint values to server as base-10 or hexadecimal strings
20. SafeUint values are arbitary-precision unsigned integers.

# Protection levels
PAYABLE - this request method credits or debit customer funds

MARKET - this request method have an impact on the underlying market

AUTHENTICATED - this request method require authentication (e.g trading)

CAPTCHA - this request method requires captcha (e.g register)

PUBLIC - this request method requires no authentication (e.g bid-ask spread)

NODB - this request method does not perform database updates (e.g price checking)

COOKIES - this request method sends session cookies

# Request methods

## void get_test_tokens() AUTHENTICATED PAYABLE
Credits test shitcoins to the user's trading account (hidden on prod server)

## void cancel_order(ulong target) AUTHENTICATED PAYABLE MARKET
Cancels an order with a given order id

## void place_order(string primary, string secondary, SafeUint price, SafeUint amount, int fill_mode, bool buy) AUTHENTICATED PAYABLE MARKET
Places an order

### primary, secondary
The base/quote part of the trading pair (e.g Dai/PolyEUBI)

### price
The maximum buy price/minimum sell price

### amount
The amount of token to buy or sell

### fill_mode
The order fill mode (0 - limit, 1 - immediate or cancel, 2 - fill or kill)

### buy
True for buy orders, false for sell orders

## [SafeUint, SafeUint] bid_ask(string primary, string secondary) PUBLIC NODB
Gets the bid and ask prices for a trading pair

## void deposit(string token) AUTHENTICATED PAYABLE
Tells OpenCEX.NET to check deposit address for deposited funds, and credit them to the customer's account if they are found.

### token
The name of the token the user want to finalize

## array([string, SafeUint]) balances() AUTHENTICATED NODB
Returns the user's balances

## string client_name() AUTHENTICATED NODB
Returns the user's username

## string eth_deposit_address() AUTHENTICATED NODB
Returns the deposit address for MintME, MATIC, and BNB

## void login(string username, string password, bool renember) PUBLIC CAPTCHA COOKIES
Logs the user in

NOTE: renember is intentionally mispelled to maintain frontend compartiability

## void create_account(string username, string password) PUBLIC CAPTCHA COOKIES
Registers a new trading account

## void logout() PUBLIC
Logs the user out of their active session

## array([string, string, SafeUint, SafeUint, SafeUint, ulong, bool]) load_active_orders() AUTHENTICATED NODB
Returns the user's active orders

## array([SafeUint x, SafeUint o, SafeUint h, SafeUint l, SafeUint c]) get_chart(string primary, string secondary) NODB
Returns the candlestick chart data for the last 60 trading days.

## void withdraw(string token, string address, SafeUint amount) AUTHENTICATED PAYABLE MARKET
Withdraws funds from client's account, or withdraw liquidity from Uniswap.NET liquidity pool

## void mint_lp(string primary, string secondary, SafeUint amount0, SafeUint amount1) AUTHENTICATED PAYABLE MARKET
Adds liquidity to an Uniswap.NET liquidity pool

### amount0
The amount of base tokens to add to liquidity pool

### amount1
The amount of quote tokens to add to liquidity pool
