package org.apache.storm.executionengine.physicalLayer.relationalOperators;

import java.io.IOException;

import org.apache.pig.StoreFuncInterface;

/**
 * This class is used to specify the actual behavior of the store operator just
 * when ready to start execution.
 */
public abstract class POStoreImpl {
  /**
   * Set up the storer
   * 
   * @param store
   *          - the POStore object
   * @throws IOException
   */
  public abstract StoreFuncInterface createStoreFunc(POStore store)
      throws IOException;

  /**
   * At the end of processing, the outputstream is closed using this method
   * 
   * @throws IOException
   */
  public void tearDown() throws IOException {
  }

  /**
   * To perform cleanup when there is an error. Uses the FileLocalizer method
   * which only deletes the file but not the dirs created with it.
   * 
   * @throws IOException
   */
  public void cleanUp() throws IOException {
  }
}
