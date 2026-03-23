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
  const data = { ...params };
  delete data.CheckMacValue;
  const sortedPairs = Object.keys(data)
    .sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))
    .map((k) => `${k}=${data[k]}`)
    .join('&');
  const raw = `HashKey=${hashKey}&${sortedPairs}&HashIV=${hashIV}`;
  return sha256Upper(toUrlEncodedForCheckMac(raw));
}

export async function onRequestPost(context) {
  try {
    const env = context.env || {};
    const formData = await context.request.formData();
    const payload = {};
    for (const [k, v] of formData.entries()) payload[k] = String(v);

    const hashKey = env.ECPAY_HASH_KEY || '5294y06JbISpM5x9';
    const hashIV = env.ECPAY_HASH_IV || 'v77hoKGq4kWxNNIS';
    const localMac = await buildCheckMacValue(payload, hashKey, hashIV);
    const remoteMac = String(payload.CheckMacValue || '').toUpperCase();
    const isMacValid = !!remoteMac && localMac === remoteMac;

    const isPaid = payload.RtnCode === '1';
    if (isMacValid && isPaid) {
      console.log('ECPay paid:', payload.MerchantTradeNo, payload.TradeNo, payload.TradeAmt);
      // TODO: Persist order status in database here.
    } else {
      console.warn('ECPay callback verify failed:', {
        merchantTradeNo: payload.MerchantTradeNo,
        rtnCode: payload.RtnCode,
        isMacValid
      });
    }

    return new Response('1|OK', {
      status: 200,
      headers: { 'content-type': 'text/plain; charset=utf-8' }
    });
  } catch (err) {
    console.error('ECPay callback error', err);
    return new Response('0|FAIL', {
      status: 500,
      headers: { 'content-type': 'text/plain; charset=utf-8' }
    });
  }
}
