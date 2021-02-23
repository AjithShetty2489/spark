package org.apache.spark.weld;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

public class WeldJNI {

  /**
   * Load the native library both in java and in weld.
   */
  private static void loadNativeLibrary(String lib) {
    System.load(lib);
    // We need to explicitly load the weld_java library for linux.
    //    Weld.loadLibrary(lib)
  }

  /**
   * Load the native libraries (weld_java & weldrt). This method first tries to load the
   * libraries from the directory defined in the 'weld.library.path' environment
   * variable. If this fails, it falls back to unpacking the libraries from the jar into
   * a temporary location, and then loading them from that temporary location.
   */
  public static void loadNativeLibraries() {
    // Try to load the library from the library path. This is easier for testing/development.
    String weldLibraryPath = "/mnt/d/ubuntu/code/weld/target/release";
    if (weldLibraryPath != null) {
      Path path = Paths.get(weldLibraryPath).toAbsolutePath();
      loadNativeLibrary(path.resolve(System.mapLibraryName("weld_java")).toString());
    }
  }

  static {
    loadNativeLibraries();
  }

  public static native long weld_context_new(long conf);

  public static native void  weld_context_free(long handle);

  public static native long weld_context_memory_usage(long handle);

  public static native long weld_value_new(long pointer);

  public static native long weld_value_pointer(long handle);

  public static native long weld_get_buffer_pointer(ByteBuffer buffer);

  public static native long weld_value_run(long handle);

  public static native long weld_value_context(long handle);

  public static native void weld_value_free(long handle);

  public static native long weld_module_compile(String code, long conf, long error);

  public static native long weld_module_run(long module, long context, long input, long error);

  public static native void weld_module_free(long module);

  public static native long weld_error_new();

  public static native void weld_error_free(long handle);

  public static native int weld_error_code(long handle);

  public static native String weld_error_message(long handle);

  public static native long weld_conf_new();

  public static native void weld_conf_free(long handle);

  public static native String weld_conf_get(long handle, String key);

  public static native void weld_conf_set(long handle, String key, String value);

  public static native void weld_load_library(String filename, long error);

  public static native void weld_set_log_level(String loglevel);
}
