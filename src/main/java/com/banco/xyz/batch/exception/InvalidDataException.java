package com.banco.xyz.batch.exception;

/**
 * Excepcion de negocio para datos legacy invalidos.
 * Se configura como skippable en los Steps (faultTolerant).
 */
public class InvalidDataException extends RuntimeException {

	public InvalidDataException(String message) {
		super(message);
	}
}
