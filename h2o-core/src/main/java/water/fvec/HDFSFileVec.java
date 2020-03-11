package water.fvec;

import water.*;

import java.io.IOException;

/**
 * Vec representation of file stored on HDFS.
 */
public final class HDFSFileVec extends FileVec {
  private HDFSFileVec(Key key, long len) {
    super(key, len, Value.HDFS);
  }

  public static Key make(String path, long size) {
    Futures fs = new Futures();
    Key key = make(path, size, fs);
    fs.blockForPending();
    return key;
  }
  public static Key make(String path, long size, Futures fs) {
    Key k = Key.make(path);
    Key k2 = Vec.newKey(k);
    new Frame(k).delete_and_lock();
    // Insert the top-level FileVec key into the store
    Vec v = new HDFSFileVec(k2,size);
    DKV.put(k2, v, fs);
    Frame fr = new Frame(k,new String[]{path},new Vec[]{v});
    fr.update();
    fr.unlock();
    return k;
  }

  @Override
  public byte[] getFirstBytes() {
    if (H2O.STORE.get(chunkKey(0)) != null) {
      // if it looks like we have the chunk cached attempt to use it instead of fetching it again
      return super.getFirstBytes();
    }
    try {
      int max = (long) _chunkSize > _len ? (int) _len : _chunkSize;
      return H2O.getPM().load(Value.HDFS, _key, 0L, max);
    } catch (IOException e) {
      throw new RuntimeException("HDFS read failed", e);
    }
  }

}
