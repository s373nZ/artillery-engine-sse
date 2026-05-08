const { EventSource } = require('eventsource');
const A = require('async');

class SSEEngine {
  constructor(script, events, helpers) {
    this.script = script;
    this.events = events;
    this.helpers = helpers;

    this.target = script.config?.target;
    this.eventSourceConfig = script.config?.engines?.['sse'] || {};

    return this;
  }

  _buildFetchOption() {
    const config = this.eventSourceConfig;
    const customHeaders = config.headers || {};
    const useHttp2 = config.http2 === true;
    const rejectUnauthorized = config.https?.rejectUnauthorized;

    const hasCustomHeaders = Object.keys(customHeaders).length > 0;
    const needsCustomFetch = hasCustomHeaders || useHttp2 || rejectUnauthorized !== undefined;

    if (!needsCustomFetch) {
      return {};
    }

    let dispatcher;
    if (useHttp2 || rejectUnauthorized !== undefined) {
      const { Agent } = require('undici');
      dispatcher = new Agent({
        allowH2: useHttp2,
        ...(rejectUnauthorized !== undefined ? { connect: { rejectUnauthorized } } : {}),
      });
    }

    return {
      fetch: (input, init) => {
        const fetchFn = dispatcher ? require('undici').fetch : globalThis.fetch;
        return fetchFn(input, {
          ...init,
          ...(hasCustomHeaders ? { headers: { ...init.headers, ...customHeaders } } : {}),
          ...(dispatcher ? { dispatcher } : {}),
        });
      },
    };
  }

  createScenario(spec, events) {
    const self = this;

    return function vu(initialContext, vuDone) {
      const steps = [];
      for (const step of spec.flow) {
        if (step === 'open' || step.open !== undefined) {

          steps.push(function open(next) {
            const rawUrl = (step.open && step.open.url) || self.target;
            const url = self.helpers.template(rawUrl, initialContext);
            const fetchOption = self._buildFetchOption();
            const es = new EventSource(url, fetchOption);
            es.addEventListener('error', (err) => {
              if (err.code) {
                events.emit('counter', `sse.error.${err.code}`, 1)
              } else {
                events.emit('counter', 'sse.error', 1);
              }
            });

            es.addEventListener('message', (_msg) => {
              events.emit('counter', 'sse.message', 1);
            });

            if (spec.onMessage) {
              // TODO: Warn if no processor function
              if (self.script.config.processor?.[spec.onMessage]) {
                es.addEventListener('message', (msg) => {
                  self.script.config.processor[spec.onMessage].call(null, msg, initialContext, events);
                });
              }
            }

            if(spec.onEvent) {
              for(const handlerSpec of spec.onEvent) {
                // TODO: Warn if no processor function
                if (self.script.config.processor?.[handlerSpec.handler]) {
                  es.addEventListener(handlerSpec.eventName, (e) => {
                    self.script.config.processor[handlerSpec.handler].call(null, e, initialContext, events);
                  });
                }
              }
            }

            es.addEventListener('open', () => {
              events.emit('counter', 'sse.open', 1);
            });
            initialContext.es = es;

            events.emit('started');
            return next(null, initialContext);
          });

        };


        if (step.log) {
          steps.push(function log(context, callback) {
            console.log(self.helpers.template(step.log, context));
            return process.nextTick(function () { callback(null, context); });
          });
        }

        if (step.think) {
          steps.push(self.helpers.createThink(step, self.script.config.defaults?.think || {}));
        }

        if (step == 'close') {
          steps.push(function close(context, next) {
            context.es?.close();
            return next(null, context);
          });
        }
      } // for

      A.waterfall(steps, (err, context) => {
        vuDone(err, context);
      });
    }
  }
}

module.exports = SSEEngine;
