import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, chmodSync, symlinkSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { inspect } from 'node:util';
import { Client, ClientError, newIdempotencyKey, stringify } from '../dist/index.js';
import { parseJson, model } from '../dist/wire.js';
import { Parser } from '../dist/stream.js';
test('u64 is exact and unsafe input numbers are rejected', () => {
  for (const n of [0n,9007199254740993n,18446744073709551615n]) {
    const value = model('OutputStats',parseJson(`{"seen":${n},"stored":0,"truncated":true}`));
    assert.equal(value.seen,n); assert.ok(stringify(value).includes(String(n)));
  }
  for (const n of ['-1','18446744073709551616','1.0','true']) assert.throws(()=>model('OutputStats',parseJson(`{"seen":${n},"stored":0,"truncated":true}`)),ClientError);
  assert.throws(()=>model('CommandInput',{argv:['true'],deadline_unix_ms:9007199254740992},false),ClientError);
  assert.throws(()=>stringify({unsafe:9007199254740992}),ClientError);
});
test('defaults, explicit null, closed models and prototype names',()=>{
  assert.equal(stringify(model('CommandInput',{argv:['true'],deadline_unix_ms:1},false)), '{"argv":["true"],"env":{},"cwd":"/","deadline_unix_ms":1,"output_limit":1048576}');
  assert.equal(stringify(model('DestroyRequest',{},false)),'{"correlation_id":null}');
  for(const extra of [{env:null},{unknown:true}])assert.throws(()=>model('CommandInput',{argv:[],deadline_unix_ms:1,...extra},false),ClientError);
  assert.throws(()=>model('SandboxList',{items:[]}),ClientError);
  assert.equal(stringify(model('SandboxList',{items:[],next_cursor:null})),'{"items":[],"next_cursor":null}');
  assert.equal(Object.getPrototypeOf(parseJson('{"__proto__":{"polluted":true}}')),null);
  assert.equal({}.polluted,undefined);
});
test('malformed and unbounded JSON fails',()=>{
  for(const raw of [Uint8Array.of(255),'{"x":1,"x":2}','NaN','1e999','"\\ud800"','['.repeat(130)+'0'+']'.repeat(130)]) assert.throws(()=>parseJson(raw),ClientError);
});
test('optional generation rejects explicit null while omission and a guard remain distinct',()=>{
  const body={argv:['true'],deadline_unix_ms:1,expected_generation:null};
  for (const decode of [false,true]) assert.throws(()=>model('CommandInput',body,decode),error=>error instanceof ClientError && error.kind===(decode?'protocol':'request'));
  assert.equal(Object.hasOwn(model('CommandInput',{argv:['true'],deadline_unix_ms:1},false),'expected_generation'),false);
  for (const generation of [0n,3n]) assert.equal(model('CommandInput',{argv:['true'],deadline_unix_ms:1,expected_generation:generation},false).expected_generation,generation);
  assert.equal(Object.hasOwn(model('ProblemBody',{title:'fixture',status:409,code:'conflict',operation_id:null}),'operation_id'),false);
});
test('keys are fresh and error diagnostics redact backend data',()=>{
  assert.notEqual(newIdempotencyKey(),newIdempotencyKey());
  const e=new ClientError('http',503,'private-backend-text','private-backend-text');
  assert.ok(!(inspect(e)+String(e)).includes('private-backend-text'));assert.equal(e.code,'unrecognized_problem_code');assert.equal(e.operationId,undefined);
});
test('SSE commits only complete bounded frames',()=>{
  const parser=new Parser();for(const b of Buffer.from('event: gap\ndata: {"code":"output_expired"}\n'))assert.equal(parser.byte(b),undefined);
  assert.equal(parser.byte(10).event,'gap');const big=new Parser();assert.throws(()=>{for(let i=0;i<65537;i++)big.byte(120)},ClientError);
});
test('configuration rejects insecure origins, loose permissions and links',()=>{
  const directory=mkdtempSync(join(tmpdir(),'hudson-sdk-'));
  try {
    const config=join(directory,'config'),credential=join(directory,'credential');
    const write=(path,data)=>{writeFileSync(path,JSON.stringify(data),{mode:0o600});chmodSync(path,0o600)};
    const creds={version:1,project_id:'prj_fixture',name:'fixture',token:'test-private-token',created_at:1,expires_at:4102444800};
    const cfg={version:1,endpoint:'https://localhost',credential_file:'credential'};write(credential,creds);
    for(const endpoint of ['http://localhost','https://user:secret@localhost','https://localhost/path','https://localhost/.','https://localhost/a/..','https://localhost/?','https://localhost/#']) {write(config,{...cfg,endpoint});assert.throws(()=>new Client(config),ClientError)}
    write(config,cfg);chmodSync(credential,0o644);assert.throws(()=>new Client(config),ClientError);
    chmodSync(credential,0o600);symlinkSync(credential,join(directory,'link'));write(config,{...cfg,credential_file:'link'});assert.throws(()=>new Client(config),ClientError);
    write(config,{...cfg,request_timeout_seconds:null});assert.throws(()=>new Client(config),ClientError);
    write(config,cfg);writeFileSync(config,'x'.repeat(65537));assert.throws(()=>new Client(config),ClientError);
  } finally {rmSync(directory,{recursive:true,force:true})}
});
