
"""
Utility functions for GWAS harmonization
"""
# === Allele and genetic utilities ===
def is_palindromic(A1, A2):
    """ Checks if two alleles are palindromic.
    Args:
        A1, A2 (Seq): Alleles (i.e. other and effect alleles)
    """
    return A1.str() == A2.revcomp().str()

def compatible_alleles_either_strand(A1, A2, B1, B2):
    """ Checks whether alleles are compatible, either on the forward or reverse
        strand
    Args:
        A1, A2 (Seq): Alleles from one source
        B1, B2 (Seq): Alleles from another source
    Returns:
        Boolean
    """
    return (compatible_alleles_forward_strand(A1, A2, B1, B2) or
            compatible_alleles_reverse_strand(A1, A2, B1, B2))


def compatible_alleles_forward_strand(A1, A2, B1, B2):
    """ Checks whether alleles are compatible on the forward strand
    Args:
        A1, A2 (Seq): Alleles from one source
        B1, B2 (Seq): Alleles from another source
    Returns:
        Boolean
    """
    return set([A1.str(), A2.str()]) == set([B1.str(), B2.str()])

def compatible_alleles_reverse_strand(A1, A2, B1, B2):
    """ Checks whether alleles are compatible on the forward strand
    Args:
        A1, A2 (Seq): Alleles from one source
        B1, B2 (Seq): Alleles from another source
    Returns:
        Boolean
    """
    return set([A1.str(), A2.str()]) == set([B1.revcomp().str(), B2.revcomp().str()])

def af_to_maf(af):
    """ Converts an allele frequency to a minor allele frequency
    Args:
        af (float or str)
    Returns:
        float
    """
    # Sometimes AF == ".", in these cases, set to 0
    try:
        af = float(af)
    except ValueError:
        af = 0.0

    if af <= 0.5:
        return af
    else:
        return 1 - af

def afs_concordant(af1, af2):
    """ Checks whether the allele frequencies of two palindromic variants are
        concordant. Concordant if both are either >0.5 or both are <0.5.
    Args:
        af1, af2 (float): Allele frequencies from two datasets
    Returns:
        Bool: True if concordant
    """
    assert isinstance(af1, float) and isinstance(af2, float)
    if (af1 >= 0.5 and af2 >= 0.5) or (af1 < 0.5 and af2 < 0.5):
        return True
    else:
        return False

# === Allele Harmonise utils ===
def harmonise_palindromic(ss_rec, vcf_rec, palin_mode, af_vcf_field, infer_maf_threshold):
    ''' Harmonises palindromic variant
    Args:
        ss_rec (SumStatRecord): object containing summary statistic record
        vcf_rec (VCFRecord): matching vcf record
    Returns:
        harmonised ss_rec
    '''

    # Mode: Infer strand mode
    if palin_mode == 'infer':

        # Extract allele frequency if argument is provided
        if af_vcf_field and af_vcf_field in vcf_rec.info:
            vcf_alt_af = float(vcf_rec.info[af_vcf_field][0])
        else:
            ss_rec.hm_code = 17
            return ss_rec

        # Discard if either MAF is greater than threshold
        if ss_rec.eaf:
            if ( af_to_maf(ss_rec.eaf) > infer_maf_threshold or
                af_to_maf(vcf_alt_af) > infer_maf_threshold ):
                ss_rec.hm_code = 18
                return ss_rec
        else:
            ss_rec.hm_code = 17
            return ss_rec

        # If EAF and alt AF are concordant, then alleles are on forward strand
        if afs_concordant(ss_rec.eaf, vcf_alt_af):

            # If alleles flipped orientation
            if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
                ss_rec.flip_beta()
                ss_rec.is_harmonised = True
                ss_rec.hm_code = 2
                return ss_rec
            # If alleles in correct orientation
            else:
                ss_rec.is_harmonised = True
                ss_rec.hm_code = 1
                return ss_rec

        # Else alleles are on the reverse strand
        else:

            # Take reverse complement of ssrec alleles
            ss_rec.revcomp_alleles()
            # If alleles flipped orientation
            if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
                ss_rec.flip_beta()
                ss_rec.is_harmonised = True
                ss_rec.hm_code = 4
                return ss_rec
            # If alleles in correct orientation
            else:
                ss_rec.is_harmonised = True
                ss_rec.hm_code = 3
                return ss_rec

    # Mode: Assume palindromic variants are on the forward strand
    elif palin_mode == 'forward':

        # If alleles flipped orientation
        if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
            ss_rec.flip_beta()
            ss_rec.is_harmonised = True
            ss_rec.hm_code = 6
            return ss_rec
        # If alleles in correct orientation
        else:
            ss_rec.is_harmonised = True
            ss_rec.hm_code = 5
            return ss_rec

    # Mode: Assume palindromic variants are on the reverse strand
    elif palin_mode == 'reverse':

        # Take reverse complement of ssrec alleles
        ss_rec.revcomp_alleles()
        # If alleles flipped orientation
        if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
            ss_rec.flip_beta()
            ss_rec.is_harmonised = True
            ss_rec.hm_code = 8
            return ss_rec
        # If alleles in correct orientation
        else:
            ss_rec.is_harmonised = True
            ss_rec.hm_code = 7
            return ss_rec

    # Mode: Drop palindromic variants
    elif palin_mode == 'drop':
        ss_rec.hm_code = 9
        return ss_rec

def harmonise_reverse_strand(ss_rec, vcf_rec):
    ''' Harmonises reverse strand variant
    Args:
        ss_rec (SumStatRecord): object containing summary statistic record
        vcf_rec (VCFRecord): matching vcf record
    Returns:
        harmonised ss_rec
    '''
    # Take reverse complement of ssrec alleles
    ss_rec.revcomp_alleles()
    # If alleles flipped orientation
    if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
        ss_rec.flip_beta()
        ss_rec.is_harmonised = True
        ss_rec.hm_code = 13
        return ss_rec
    # If alleles in correct orientation
    else:
        ss_rec.is_harmonised = True
        ss_rec.hm_code = 12
        return ss_rec

def harmonise_forward_strand(ss_rec, vcf_rec):
    ''' Harmonises forward strand variant
    Args:
        ss_rec (SumStatRecord): object containing summary statistic record
        vcf_rec (VCFRecord): matching vcf record
    Returns:
        harmonised ss_rec
    '''
    # If alleles flipped orientation
    if ss_rec.effect_al.str() == vcf_rec.ref_al.str():
        ss_rec.flip_beta()
        ss_rec.is_harmonised = True
        ss_rec.hm_code = 11
        return ss_rec
    # If alleles in correct orientation
    else:
        ss_rec.is_harmonised = True
        ss_rec.hm_code = 10
        return ss_rec

