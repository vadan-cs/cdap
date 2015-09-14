/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.artifact.InvalidPluginConfigException;
import co.cask.cdap.internal.lang.FieldVisitor;
import co.cask.cdap.internal.lang.Fields;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.concurrent.ExecutionException;

/**
 * This class helps creating new instances of plugins. It also contains a ClassLoader cache to
 * save ClassLoader creation.
 *
 * This class implements {@link Closeable} as well for cleanup of temporary directories created for the ClassLoaders.
 */
public class PluginInstantiator implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PluginInstantiator.class);

  private final LoadingCache<Location, ClassLoader> locationClassLoaders;
  private final LoadingCache<ArtifactDescriptor, ClassLoader> classLoaders;
  private final InstantiatorFactory instantiatorFactory;
  private final File tmpDir;
  private final File pluginDir;
  private final ClassLoader parentClassLoader;

  public PluginInstantiator(CConfiguration cConf, ClassLoader parentClassLoader) {
    this.instantiatorFactory = new InstantiatorFactory(false);
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File prefix = cConf.get(Constants.AppFabric.MRTASK_PLUGIN_DIR) != null ?
      new File(cConf.get(Constants.AppFabric.MRTASK_PLUGIN_DIR), cConf.get(Constants.CFG_LOCAL_DATA_DIR)) :
      new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));

    this.pluginDir = new File(prefix, "namespaces");
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    this.locationClassLoaders = CacheBuilder.newBuilder()
                                            .removalListener(new LocationClassLoaderRemovalListener())
                                            .build(new LocationClassLoaderCacheLoader());
    this.classLoaders = CacheBuilder.newBuilder()
                                    .removalListener(new ClassLoaderRemovalListener())
                                    .build(new ClassLoaderCacheLoader());
    this.parentClassLoader = PluginClassLoader.createParent(parentClassLoader);
  }

  /**
   * Returns a {@link ClassLoader} for the given artifact.
   *
   * @param artifactDescriptor descriptor for the artifact
   * @throws IOException if failed to expand the artifact jar to create the plugin ClassLoader
   *
   * @see PluginClassLoader
   */
  public ClassLoader getArtifactClassLoader(ArtifactDescriptor artifactDescriptor) throws IOException {
    try {
      return classLoaders.get(artifactDescriptor);
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }

  /**
   * Returns a {@link ClassLoader} for the given artifact identified by the given Location.
   *
   * @param location Location of the artifact
   * @throws IOException if failed to expand the artifact jar to create the plugin ClassLoader
   *
   * @see PluginClassLoader
   */
  public ClassLoader getArtifactClassLoader(Location location) throws IOException {
    try {
      return locationClassLoaders.get(location);
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }

  /**
   * Loads and returns the {@link Class} of the given plugin class.
   *
   * @param artifactDescriptor descriptor for the artifact the plugin is from.
   *                           It is used for creating the ClassLoader for the plugin.
   * @param pluginClass information about the plugin class
   * @param <T> Type of the plugin
   * @return the plugin Class
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> loadClass(ArtifactDescriptor artifactDescriptor,
                                PluginClass pluginClass) throws IOException, ClassNotFoundException {
    return (Class<T>) getArtifactClassLoader(artifactDescriptor).loadClass(pluginClass.getClassName());
  }

  @SuppressWarnings("unchecked")
  public <T> Class<T> loadClass(Location location,
                                PluginClass pluginClass) throws IOException, ClassNotFoundException {
    return (Class<T>) getArtifactClassLoader(location).loadClass(pluginClass.getClassName());
  }

  /**
   * Creates a new instance of the given plugin class.
   *
   * @param artifactDescriptor descriptor for the artifact the plugin is from.
   *                           It is used for creating the ClassLoader for the plugin.
   * @param pluginClass information about the plugin class. The plugin instance will be instantiated based on this.
   * @param properties properties to populate into the {@link PluginConfig} of the plugin instance
   * @param <T> Type of the plugin
   * @return a new plugin instance
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  @SuppressWarnings("unchecked")
  public <T> T newInstance(ArtifactDescriptor artifactDescriptor, PluginClass pluginClass,
                           PluginProperties properties) throws IOException, ClassNotFoundException {
    return newInstance(getArtifactClassLoader(artifactDescriptor), pluginClass, properties);
  }

  /**
   * Creates a new instance of the given plugin class by identifying the class loader using the given location
   * @param location location of the plugin artifact jar
   * @param pluginClass information about the plugin class. The plugin instnace will be instantiated based on this.
   * @param properties properties to populate into the {@link PluginConfig} of the plugin instnace
   * @param <T> Type of the plugin
   * @return a new plugin instance
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  @SuppressWarnings("unchecked")
  public <T> T newInstance(Location location, PluginClass pluginClass,
                           PluginProperties properties) throws IOException, ClassNotFoundException {
    return newInstance(getArtifactClassLoader(location), pluginClass, properties);
  }

  private <T> T newInstance(ClassLoader classLoader, PluginClass pluginClass, PluginProperties properties)
    throws ClassNotFoundException {
    TypeToken<?> pluginType = TypeToken.of(classLoader.loadClass(pluginClass.getClassName()));

    try {
      String configFieldName = pluginClass.getConfigFieldName();
      // Plugin doesn't have config. Simply return a new instance.
      if (configFieldName == null) {
        return (T) instantiatorFactory.get(pluginType).create();
      }

      // Create the config instance
      Field field = Fields.findField(pluginType.getType(), configFieldName);
      TypeToken<?> configFieldType = pluginType.resolveType(field.getGenericType());
      Object config = instantiatorFactory.get(configFieldType).create();
      Reflections.visit(config, configFieldType.getType(),
                        new ConfigFieldSetter(pluginClass, properties));

      // Create the plugin instance
      return newInstance(pluginType, field, configFieldType, config);
    } catch (NoSuchFieldException e) {
      throw new InvalidPluginConfigException("Config field not found in plugin class: " + pluginClass, e);
    } catch (IllegalAccessException e) {
      throw new InvalidPluginConfigException("Failed to set plugin config field: " + pluginClass, e);
    }
  }

  /**
   * Creates a new plugin instance and optionally setup the {@link PluginConfig} field.
   */
  @SuppressWarnings("unchecked")
  private <T> T newInstance(TypeToken<?> pluginType, Field configField,
                            TypeToken<?> configFieldType, Object config) throws IllegalAccessException {
    // See if the plugin has a constructor that takes the config type.
    // Need to loop because we need to resolve the constructor parameter type from generic.
    for (Constructor<?> constructor : pluginType.getRawType().getConstructors()) {
      Type[] parameterTypes = constructor.getGenericParameterTypes();
      if (parameterTypes.length != 1) {
        continue;
      }
      if (configFieldType.equals(pluginType.resolveType(parameterTypes[0]))) {
        constructor.setAccessible(true);
        try {
          // Call the plugin constructor to construct the instance
          return (T) constructor.newInstance(config);
        } catch (Exception e) {
          // Failed to instantiate. Resort to field injection
          LOG.warn("Failed to invoke plugin constructor {}. Resort to config field injection.", constructor);
          break;
        }
      }
    }

    // No matching constructor found, do field injection.
    T plugin = (T) instantiatorFactory.get(pluginType).create();
    configField.setAccessible(true);
    configField.set(plugin, config);
    return plugin;
  }

  @Override
  public void close() throws IOException {
    // Cleanup the ClassLoader cache and the temporary directoy for the expanded plugin jar.
    classLoaders.invalidateAll();
    locationClassLoaders.invalidateAll();
    if (parentClassLoader instanceof Closeable) {
      Closeables.closeQuietly((Closeable) parentClassLoader);
    }
    try {
      DirUtils.deleteDirectoryContents(tmpDir);
    } catch (IOException e) {
      // It's the cleanup step. Nothing much can be done if cleanup failed.
      LOG.warn("Failed to delete directory {}", tmpDir);
    }
  }

  /**
   * A CacheLoader for creating plugin ClassLoader.
   */
  private final class ClassLoaderCacheLoader extends CacheLoader<ArtifactDescriptor, ClassLoader> {

    @Override
    public ClassLoader load(ArtifactDescriptor key) throws Exception {
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      String artifactFile = String.format("%s/artifacts/%s/%s", key.getArtifact().getNamespace().getId(),
                                          key.getArtifact().getName(), key.getLocation().getName());
      File artifact = new File(pluginDir, artifactFile);
      BundleJarUtil.unpackProgramJar(Locations.toLocation(artifact), unpackedDir);
      return new PluginClassLoader(unpackedDir, parentClassLoader);
    }
  }

  /**
   * A RemovalListener for closing plugin ClassLoader.
   */
  private static final class ClassLoaderRemovalListener implements RemovalListener<ArtifactDescriptor, ClassLoader> {

    @Override
    public void onRemoval(RemovalNotification<ArtifactDescriptor, ClassLoader> notification) {
      ClassLoader cl = notification.getValue();
      if (cl instanceof Closeable) {
        Closeables.closeQuietly((Closeable) cl);
      }
    }
  }

  private final class LocationClassLoaderCacheLoader extends CacheLoader<Location, ClassLoader> {

    @Override
    public ClassLoader load(Location location) throws Exception {
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      BundleJarUtil.unpackProgramJar(location, unpackedDir);
      return new PluginClassLoader(unpackedDir, parentClassLoader);
    }
  }

  /**
   * A RemovalListener for closing plugin ClassLoader.
   */
  private static final class LocationClassLoaderRemovalListener implements RemovalListener<Location, ClassLoader> {

    @Override
    public void onRemoval(RemovalNotification<Location, ClassLoader> notification) {
      ClassLoader cl = notification.getValue();
      if (cl instanceof Closeable) {
        Closeables.closeQuietly((Closeable) cl);
      }
    }
  }


  /**
   * A {@link FieldVisitor} for setting values into {@link PluginConfig} object based on {@link PluginProperties}.
   */
  private static final class ConfigFieldSetter extends FieldVisitor {
    private final PluginClass pluginClass;
    private final PluginProperties properties;
    private final ArtifactDescriptor artifactDescriptor;

    public ConfigFieldSetter(PluginClass pluginClass, PluginProperties properties) {
      this(pluginClass, null, properties);
    }

    public ConfigFieldSetter(PluginClass pluginClass, ArtifactDescriptor artifactDescriptor,
                             PluginProperties properties) {
      this.pluginClass = pluginClass;
      this.artifactDescriptor = artifactDescriptor;
      this.properties = properties;
    }

    @Override
    public void visit(Object instance, Type inspectType, Type declareType, Field field) throws Exception {
      TypeToken<?> declareTypeToken = TypeToken.of(declareType);

      if (PluginConfig.class.equals(declareTypeToken.getRawType())) {
        if (field.getName().equals("properties")) {
          field.set(instance, properties);
        }
        return;
      }

      Name nameAnnotation = field.getAnnotation(Name.class);
      String name = nameAnnotation == null ? field.getName() : nameAnnotation.value();
      PluginPropertyField pluginPropertyField = pluginClass.getProperties().get(name);
      if (pluginPropertyField.isRequired() && !properties.getProperties().containsKey(name)) {
        throw new IllegalArgumentException("Missing required plugin property " + name
                                             + " for " + pluginClass.getName() + " in artifact " + artifactDescriptor);
      }
      String value = properties.getProperties().get(name);
      if (pluginPropertyField.isRequired() || value != null) {
        field.set(instance, convertValue(declareTypeToken.resolveType(field.getGenericType()), value));
      }
    }

    /**
     * Converts string value into value of the fieldType.
     */
    private Object convertValue(TypeToken<?> fieldType, String value) throws Exception {
      // Currently we only support primitive, wrapped primitive and String types.
      Class<?> rawType = fieldType.getRawType();

      if (String.class.equals(rawType)) {
        return value;
      }

      if (rawType.isPrimitive()) {
        rawType = Primitives.wrap(rawType);
      }

      if (Primitives.isWrapperType(rawType)) {
        Method valueOf = rawType.getMethod("valueOf", String.class);
        try {
          return valueOf.invoke(null, value);
        } catch (InvocationTargetException e) {
          if (e.getCause() instanceof NumberFormatException) {
            // if exception is due to wrong value for integer/double conversion
            throw new InvalidPluginConfigException(String.format("valueOf operation on %s failed", value),
                                                   e.getCause());
          }
          throw e;
        }
      }

      throw new UnsupportedTypeException("Only primitive and String types are supported");
    }
  }
}
