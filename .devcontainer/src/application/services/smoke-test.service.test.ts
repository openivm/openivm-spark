import assert from 'node:assert/strict';
import test from 'node:test';

import { CommandSpec, ProcessRunner } from '../ports/process-runner.js';
import {
  smokeContainerLabel,
  sourceRepository,
  supportedImagePlatform,
  workspaceRoot,
} from '../../infrastructure/config/devcontainer-config.js';
import { EnvironmentConfiguration } from '../../infrastructure/config/environment.js';
import { NodeFileSystem } from '../../infrastructure/filesystem/node-file-system.js';
import { SmokeTestService } from './smoke-test.service.js';

test('cleanup removes only labeled stale smoke containers', () => {
  const calls: CommandSpec[] = [];
  const removed = createService({
    run: (spec) => {
      calls.push(spec);
      if (spec.args?.[0] === 'ps') {
        return { status: 0, stderr: '', stdout: 'container-a\ncontainer-b\n' };
      }
      return { status: 0, stderr: '', stdout: '' };
    },
  }).cleanup({ logger: () => undefined });

  assert.deepEqual(removed, ['container-a', 'container-b']);
  assert.deepEqual(calls[0]?.args, [
    'ps',
    '--all',
    '--quiet',
    '--filter',
    `label=${smokeContainerLabel}`,
  ]);
  assert.deepEqual(calls[1]?.args, ['rm', '--force', 'container-a', 'container-b']);
});

test('cleanup is idempotent when no labeled smoke containers exist', () => {
  const calls: CommandSpec[] = [];
  const logs: string[] = [];
  const removed = createService({
    run: (spec) => {
      calls.push(spec);
      return { status: 0, stderr: '', stdout: '' };
    },
  }).cleanup({ logger: (message) => logs.push(message) });

  assert.deepEqual(removed, []);
  assert.equal(calls.length, 1);
  assert.deepEqual(logs, ['No stale devcontainer smoke containers found.']);
});

test('cleanup tolerates containers disappearing after discovery', () => {
  const removed = createService({
    run: (spec) =>
      spec.args?.[0] === 'ps'
        ? { status: 0, stderr: '', stdout: 'container-a\n' }
        : { status: 1, stderr: 'Error: No such container: container-a', stdout: '' },
  }).cleanup({ logger: () => undefined });

  assert.deepEqual(removed, ['container-a']);
});

test('smoke runs remove only their own uniquely named containers', () => {
  const calls: CommandSpec[] = [];
  const fileSystem = new NodeFileSystem();
  const exists = fileSystem.exists.bind(fileSystem);
  fileSystem.exists = (path) => path === '/var/run/docker.sock' || exists(path);
  fileSystem.groupId = () => 1;
  const service = new SmokeTestService(
    {
      run: (spec) => {
        calls.push(spec);
        const format = spec.args?.[3];
        const stdout =
          spec.args?.[0] === 'image'
            ? format === '{{.Os}}/{{.Architecture}}'
              ? supportedImagePlatform
              : sourceRepository
            : '';
        return { status: 0, stderr: '', stdout };
      },
    },
    fileSystem,
    new EnvironmentConfiguration(fileSystem),
    {
      containerLabel: smokeContainerLabel,
      imagePlatform: supportedImagePlatform,
      root: workspaceRoot,
      sourceRepository,
    },
    () => undefined,
  );
  service.testImage('image');
  service.testImage('image');
  assert.equal(
    calls.some((call) => call.args?.[0] === 'ps'),
    false,
  );
  const runs = calls.filter((call) => call.args?.[0] === 'run');
  const removals = calls.filter((call) => call.args?.[0] === 'rm');
  const names = runs.map((call) => call.args?.[call.args.indexOf('--name') + 1]);
  assert.equal(new Set(names).size, 2);
  assert.deepEqual(
    removals.map((call) => call.args),
    names.map((name) => ['rm', '--force', name]),
  );
});

test('native runtime smoke loads the pinned extension through the CLI', () => {
  const checks = createService({
    run: () => ({ status: 0, stderr: '', stdout: '' }),
  }).runtimeChecks();
  const native = checks.find((check) => check.name === 'JDKs, sbt, and native OpenIVM artifacts');
  assert.ok(native);
  assert.match(native.script, /LOAD '\/opt\/openivm\/openivm\.duckdb_extension'/u);
  assert.match(native.script, /SIMPLE_PROJECTION/u);
});

function createService(runner: ProcessRunner): SmokeTestService {
  const fileSystem = new NodeFileSystem();
  return new SmokeTestService(
    runner,
    fileSystem,
    new EnvironmentConfiguration(fileSystem),
    {
      containerLabel: smokeContainerLabel,
      imagePlatform: supportedImagePlatform,
      root: workspaceRoot,
      sourceRepository,
    },
    () => undefined,
  );
}
