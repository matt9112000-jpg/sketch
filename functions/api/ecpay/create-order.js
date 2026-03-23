function toUrlEncodedForCheckMac(raw) {
  return encodeURIComponent(raw)
    .toLowerCase()
    .replace(/%20/g, '+')
    .replace(/%2d/g, '-')
    .replace(/%5f/g, '_')
    .replace(/%2e/g, '.')
    .replace(/%21/g, '!')
    .replace(/%2a/g, '*')
    .replace(/%28/g, '(')
    .replace(/%29/g, ')');
}

async function sha256Upper(text) {
  const bytes = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  const arr = Array.from(new Uint8Array(digest));
  return arr.map((b) => b.toString(16).padStart(2, '0')).join('').toUpperCase();
}

async function buildCheckMacValue(params, hashKey, hashIV) {
  const sortedPairs = Object.keys(params)
    .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))
    .map((k) => `${k}=${params[k]}`)
    .join('&');
  const raw = `HashKey=${hashKey}&${sortedPairs}&HashIV=${hashIV}`;
  return sha256Upper(toUrlEncodedForCheckMac(raw));
}

function makeTradeNo() {
  const ts = Date.now().toString().slice(-10);
  const rand = Math.random().toString(36).slice(2, 8).toUpperCase();
  return `IS${ts}${rand}`.slice(0, 20);
}

function makeTradeDate() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}/${pad(d.getMonth() + 1)}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' }
  });
}

export async function onRequestPost(context) {
  try {
    const env = context.env || {};
    const body = await context.request.json();
    const amount = Number(body.amount || 0);
    const itemName = String(body.itemName || '001 — CCC');

    if (!Number.isFinite(amount) || amount <= 0) {
      return json({ error: 'Invalid amount' }, 400);
    }

    const merchantID = env.ECPAY_MERCHANT_ID || '2000132';
    const hashKey = env.ECPAY_HASH_KEY || '5294y06JbISpM5x9';
    const hashIV = env.ECPAY_HASH_IV || 'v77hoKGq4kWxNNIS';
    const isStage = (env.ECPAY_STAGE || 'true') !== 'false';
    const action = isStage
      ? 'https://payment-stage.ecpay.com.tw/Cashier/AioCheckOut/V5'
      : 'https://payment.ecpay.com.tw/Cashier/AioCheckOut/V5';

    const origin = new URL(context.request.url).origin;
    const returnURL = env.ECPAY_RETURN_URL || `${origin}/api/ecpay/callback`;
    const orderResultURL = env.ECPAY_ORDER_RESULT_URL || `${origin}/shop.html?paid=1`;
    const clientBackURL = env.ECPAY_CLIENT_BACK_URL || `${origin}/shop.html`;
    const paymentInfoURL = env.ECPAY_PAYMENT_INFO_URL || returnURL;

    const fields = {
      MerchantID: merchantID,
      MerchantTradeNo: makeTradeNo(),
      MerchantTradeDate: makeTradeDate(),
      PaymentType: 'aio',
      TotalAmount: String(Math.round(amount)),
      TradeDesc: 'Insufficient Space Order',
      ItemName: itemName,
      ReturnURL: returnURL,
      ChoosePayment: 'ALL',
      EncryptType: '1',
      OrderResultURL: orderResultURL,
      ClientBackURL: clientBackURL,
      PaymentInfoURL: paymentInfoURL
    };

    fields.CheckMacValue = await buildCheckMacValue(fields, hashKey, hashIV);
    return json({ action, fields });
  } catch (err) {
    return json({ error: err && err.message ? err.message : 'Unknown error' }, 500);
  }
}
