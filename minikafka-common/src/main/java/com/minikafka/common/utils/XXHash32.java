package com.minikafka.common.utils;

public class XXHash32 {
    // xxHash32 Prime Constants
    private static final int PRIME1 = (int) 2654435761L;
    private static final int PRIME2 = (int) 2246822519L;
    private static final int PRIME3 = (int) 3266489917L;
    private static final int PRIME4 = 668265263;
    private static final int PRIME5 = 374761393;

    public static int hash(byte[] input, int seed) {
        int length = input.length;
        int h32;
        int index = 0;

        if (length >= 16) {
            int limit = length - 16;
            int v1 = seed + PRIME1 + PRIME2;
            int v2 = seed + PRIME2;
            int v3 = seed + 0;
            int v4 = seed - PRIME1;

            // Process 16 bytes at a time
            while (index <= limit) {
                v1 += getInt(input, index) * PRIME2;
                v1 = Integer.rotateLeft(v1, 13);
                v1 *= PRIME1;
                index += 4;

                v2 += getInt(input, index) * PRIME2;
                v2 = Integer.rotateLeft(v2, 13);
                v2 *= PRIME1;
                index += 4;

                v3 += getInt(input, index) * PRIME2;
                v3 = Integer.rotateLeft(v3, 13);
                v3 *= PRIME1;
                index += 4;

                v4 += getInt(input, index) * PRIME2;
                v4 = Integer.rotateLeft(v4, 13);
                v4 *= PRIME1;
                index += 4;
            }

            h32 = Integer.rotateLeft(v1, 1) +
                  Integer.rotateLeft(v2, 7) +
                  Integer.rotateLeft(v3, 12) +
                  Integer.rotateLeft(v4, 18);
        } else {
            h32 = seed + PRIME5;
        }

        h32 += length;

        // Process remaining bytes
        while (index <= length - 4) {
            h32 += getInt(input, index) * PRIME3;
            h32 = Integer.rotateLeft(h32, 17) * PRIME4;
            index += 4;
        }

        while (index < length) {
            h32 += (input[index] & 0xFF) * PRIME5;
            h32 = Integer.rotateLeft(h32, 11) * PRIME1;
            index++;
        }

        // Final avalanche phase
        h32 ^= h32 >>> 15;
        h32 *= PRIME2;
        h32 ^= h32 >>> 13;
        h32 *= PRIME3;
        h32 ^= h32 >>> 16;

        return h32;
    }

    // Utility to read 4 bytes into an int (Little-Endian)
    private static int getInt(byte[] b, int i) {
        return (b[i] & 0xFF) |
               ((b[i + 1] & 0xFF) << 8) |
               ((b[i + 2] & 0xFF) << 16) |
               ((b[i + 3] & 0xFF) << 24);
    }
}