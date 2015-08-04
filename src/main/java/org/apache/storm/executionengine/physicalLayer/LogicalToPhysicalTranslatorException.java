package org.apache.storm.executionengine.physicalLayer;

import org.apache.pig.impl.plan.VisitorException;

public class LogicalToPhysicalTranslatorException extends VisitorException {

  private static final long serialVersionUID = 1L;

  /**
   * Create a new LogicalToPhysicalTranslatorException with null as the error
   * message.
   */
  public LogicalToPhysicalTranslatorException() {
    super();
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   */
  public LogicalToPhysicalTranslatorException(String message) {
    super(message);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified cause.
   * 
   * @param cause
   *          - The cause (which is saved for later retrieval by the
   *          <link>Throwable.getCause()</link> method) indicating the source of
   *          this exception. A null value is permitted, and indicates that the
   *          cause is nonexistent or unknown.
   */
  public LogicalToPhysicalTranslatorException(Throwable cause) {
    super(cause);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param cause
   *          - The cause (which is saved for later retrieval by the
   *          <link>Throwable.getCause()</link> method) indicating the source of
   *          this exception. A null value is permitted, and indicates that the
   *          cause is nonexistent or unknown.
   */
  public LogicalToPhysicalTranslatorException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param errCode
   *          - The error code shown to the user
   */
  public LogicalToPhysicalTranslatorException(String message, int errCode) {
    super(message, errCode);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param errCode
   *          - The error code shown to the user
   * @param cause
   *          - The cause (which is saved for later retrieval by the
   *          <link>Throwable.getCause()</link> method) indicating the source of
   *          this exception. A null value is permitted, and indicates that the
   *          cause is nonexistent or unknown.
   */
  public LogicalToPhysicalTranslatorException(String message, int errCode,
      Throwable cause) {
    super(message, errCode, cause);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param errCode
   *          - The error code shown to the user
   * @param errSrc
   *          - The error source
   */
  public LogicalToPhysicalTranslatorException(String message, int errCode,
      byte errSrc) {
    super(message, errCode, errSrc);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param errCode
   *          - The error code shown to the user
   * @param errSrc
   *          - The error source
   * @param cause
   *          - The cause (which is saved for later retrieval by the
   *          <link>Throwable.getCause()</link> method) indicating the source of
   *          this exception. A null value is permitted, and indicates that the
   *          cause is nonexistent or unknown.
   */
  public LogicalToPhysicalTranslatorException(String message, int errCode,
      byte errSrc, Throwable cause) {
    super(message, errCode, errSrc, cause);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param errCode
   *          - The error code shown to the user
   * @param retry
   *          - If the exception is retriable or not
   */
  public LogicalToPhysicalTranslatorException(String message, int errCode,
      boolean retry) {
    super(message, errCode, retry);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param errCode
   *          - The error code shown to the user
   * @param errSrc
   *          - The error source
   * @param retry
   *          - If the exception is retriable or not
   */
  public LogicalToPhysicalTranslatorException(String message, int errCode,
      byte errSrc, boolean retry) {
    super(message, errCode, errSrc, retry);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message, error code, error source, retriable or not, detalied message for
   * the developer and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param errCode
   *          - The error code shown to the user
   * @param errSrc
   *          - The error source
   * @param retry
   *          - If the exception is retriable or not
   * @param detailedMsg
   *          - The detailed message shown to the developer
   */
  public LogicalToPhysicalTranslatorException(String message, int errCode,
      byte errSrc, boolean retry, String detailedMsg) {
    super(message, errCode, errSrc, retry, detailedMsg);
  }

  /**
   * Create a new LogicalToPhysicalTranslatorException with the specified
   * message, error code, error source, retriable or not, detalied message for
   * the developer and cause.
   * 
   * @param message
   *          - The error message (which is saved for later retrieval by the
   *          <link>Throwable.getMessage()</link> method) shown to the user
   * @param errCode
   *          - The error code shown to the user
   * @param errSrc
   *          - The error source
   * @param retry
   *          - If the exception is retriable or not
   * @param detailedMsg
   *          - The detailed message shown to the developer
   * @param cause
   *          - The cause (which is saved for later retrieval by the
   *          <link>Throwable.getCause()</link> method) indicating the source of
   *          this exception. A null value is permitted, and indicates that the
   *          cause is nonexistent or unknown.
   */
  public LogicalToPhysicalTranslatorException(String message, int errCode,
      byte errSrc, boolean retry, String detailedMsg, Throwable cause) {
    super(message, errCode, errSrc, retry, detailedMsg, cause);
  }

}
