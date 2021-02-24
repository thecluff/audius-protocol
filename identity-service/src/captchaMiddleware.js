const models = require('./models')

const verifyAndRecordCaptcha = async ({ token, walletAddress, url, logger, captcha }) => {
  let score, ok, hostname
  if (token) {
    ({ score, ok, hostname } = await captcha.verify(token))

    try {
      models.BotScores.create({
        walletAddress,
        recaptchaScore: score,
        recaptchaContext: url,
        recaptchaHostname: hostname
      })
    } catch (e) {
      logger.error('CAPTCHA - Error with adding recaptcha score', e)
    }

    // TODO: Make middleware return errorResponse later
    if (!ok) logger.warn('CAPTCHA - Failed captcha')
  } else {
    logger.warn('CAPTCHA - No captcha found on request')
  }
}

async function captchaMiddleware (req, res, next) {
  const libs = req.app.get('audiusLibs')

  verifyAndRecordCaptcha({
    token: req.body.token,
    walletAddress: req.body.walletAddress,
    url: req.url,
    logger: req.logger,
    captcha: libs.captcha
  })

  next()
}

module.exports = captchaMiddleware
