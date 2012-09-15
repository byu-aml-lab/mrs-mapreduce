//starting at the given index, samples from the halton sequence size times
//returns the number of points inside the unit circle so that pi can be
//approximating using 4 * inside / size
long long halton_darts(long long index, long long size) {
    // indexes
    int j;
    long long k, i;

    // halton data
    double x0, x1;
    double q0[63], q1[40];
    int d0[63], d1[40];
    double x, y;

    // counters
    long long inside = 0;

    x0 = 0;
    q0[0] = 1.0 / 2;
    d0[0] = index % 2;
    k = (index - d0[0]) / 2;
    x0 += d0[0] * q0[0];
    for(j = 1; j < 63; ++j) {
        q0[j] = q0[j - 1] / 2;
        d0[j] = k % 2;
        k = (k - d0[j]) / 2;
        x0 += d0[j] * q0[j];
    }

    x1 = 0;
    q1[0] = 1.0 / 3;
    d1[0] = index % 3;
    k = (index - d1[0]) / 3;
    x1 += d1[0] * q1[0];
    for(j = 1; j < 40; ++j) {
        q1[j] = q1[j - 1] / 3;
        d1[j] = k % 3;
        k = (k - d1[j]) / 3;
        x1 += d1[j] * q1[j];
    }

    for(i = 0; i < size; ++i) {
        ++index;

        for(j = 0; j < 63; ++j) {
            ++d0[j];
            x0 += q0[j];
            if(d0[j] < 2)
                break;
            d0[j] = 0;
            x0 -= (j == 0 ? 1.0 : q0[j - 1]);
        }

        for(j = 0; j < 40; ++j) {
            ++d1[j];
            x1 += q1[j];
            if(d1[j] < 3)
                break;
            d1[j] = 0;
            x1 -= (j == 0 ? 1.0 : q1[j - 1]);
        }

        x = x0 - .5;
        y = x1 - .5;

        if(x * x + y * y <= .25)
            ++inside;
    }

    return inside;
}
