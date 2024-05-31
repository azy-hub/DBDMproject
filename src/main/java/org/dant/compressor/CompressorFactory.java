package org.dant.compressor;

public class CompressorFactory {

    public enum Type {
        BYTE_COMPRESSOR, NO_COMPRESSION, SNAPPY
    }

    private CompressorFactory() {
    }

    public static Compressor changeCompressor(Type type) {
        switch (type) {
            case BYTE_COMPRESSOR :
                CompressorSingleton.compressor = new ByteCompressor();
                return CompressorSingleton.compressor;
            case NO_COMPRESSION :
                CompressorSingleton.compressor = new NoCompressor();
                return CompressorSingleton.compressor;
            case SNAPPY:
                CompressorSingleton.compressor = new SnappyCompressor();
                return CompressorSingleton.compressor;
            default :
                return CompressorSingleton.compressor;
        }
    }

    public static Compressor get() {
        return CompressorSingleton.compressor;
    }

    private static class CompressorSingleton {
        private static Compressor compressor = new NoCompressor();
    }

}
