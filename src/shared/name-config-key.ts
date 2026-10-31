import { ConfigProvider, Effect } from "effect";
import type { Exit } from "effect";

export const nameConfigKey = (configPath: readonly PropertyKey[]) =>
  configPath.map(String).join(".");

export const nameEnvironmentConfigVariable = (configPath: readonly PropertyKey[]) =>
  configPath.map(String).join("_");

const runtimeConfigProviders = new WeakSet<ConfigProvider.ConfigProvider>();

export const markRuntimeConfigProvider = (provider: ConfigProvider.ConfigProvider) => {
  runtimeConfigProviders.add(provider);
  return provider;
};

export const observeConfigLoads =
  (
    onLoad: (load: {
      readonly configPath: readonly PropertyKey[];
      readonly readsRuntimeConfigProvider: boolean;
      readonly loaded: Exit.Exit<ConfigProvider.Node | undefined, ConfigProvider.SourceError>;
    }) => void,
  ) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    Effect.gen(function* () {
      const provider = yield* ConfigProvider.ConfigProvider;
      const readsRuntimeConfigProvider = runtimeConfigProviders.has(provider);
      const observed = ConfigProvider.make((configPath) =>
        provider.load(configPath).pipe(
          Effect.onExit((loaded) =>
            Effect.sync(() => {
              onLoad({ configPath, readsRuntimeConfigProvider, loaded });
            }),
          ),
        ),
      );
      if (readsRuntimeConfigProvider) {
        markRuntimeConfigProvider(observed);
      }
      return yield* Effect.provideService(effect, ConfigProvider.ConfigProvider, observed);
    });
