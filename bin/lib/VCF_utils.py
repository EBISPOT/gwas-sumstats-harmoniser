"""
VCF data handling and processing functions
"""

from lib.VCFRecord import VCFRecord
from lib.Allele_utils import compatible_alleles_either_strand, af_to_maf

# === Extract matching VCF record ===
def extract_matching_record_from_vcf_records(ss_rec, vcf_recs):
    ''' Extracts the vcf record that matches the summary stat record.
    Args:
        ss_rec (SumStatRecord): object containing summary statistic record
        vcf_recs (list of VCFRecords): list containing vcf records
    Returns:
        tuple(
            a single VCFRecord or None,
            output code
            )
    '''

    # Discard if there are no records
    if len(vcf_recs) == 0:
        return (None, 15)

    # Remove alt alleles that don't match sum stat alleles
    for i in range(len(vcf_recs)):
        non_matching_alts = find_non_matching_alleles(ss_rec, vcf_recs[i])
        for alt in non_matching_alts:
            vcf_recs[i] = vcf_recs[i].remove_alt_al(alt)

    # Remove records that don't have any matching alt alleles
    vcf_recs = [vcf_rec for vcf_rec in vcf_recs if vcf_rec.n_alts() > 0]

    # If there are multiple matching records, resolve using rsid
    if len(vcf_recs) > 1:
        vcf_recs = [vcf_rec for vcf_rec in vcf_recs if vcf_rec.id == ss_rec.rsid]

    # Discard ss_rec if there are no valid vcf_recs
    if len(vcf_recs) == 0:
        return (None, 15)

    # Discard ss_rec if there are multiple records
    if len(vcf_recs) > 1:
        return (None, 16)

    # Given that there is now 1 record, use that
    vcf_rec = vcf_recs[0]

    # Discard ssrec if there are multiple matching alleles
    if vcf_rec.n_alts() > 1:
        return (None, 16)

    return (vcf_rec, None)

def find_non_matching_alleles(sumstat_rec, vcf_rec):
    """ For each vcfrec ref-alt pair check whether it matches either the
        forward or reverse complement of the sumstat alleles.
    Args:
        sumstat_rec (SumStatRecord)
        vcf_rec (VCFRecord)
    Returns:
        list of alt alleles to remove
    """
    alts_to_remove = []
    for ref, alt in vcf_rec.yeild_alleles():
        if not compatible_alleles_either_strand(sumstat_rec.other_al,
                                                sumstat_rec.effect_al,
                                                ref,
                                                alt):
            alts_to_remove.append(alt)
    return alts_to_remove

def get_vcf_records(tbx, chrom, pos):
    """ Uses tabix to query VCF file. Parses info from record.
    Args:
        in_vcf (str): vcf file
        chrom (str): chromosome
        pos (int): base pair position
    Returns:
        list of VCFRecords
    """
    #######YUE################
    region = f"{chrom}:{pos}-{int(pos)+1}"  # input is 0-based
    result = tbx(region)
    # each records returned by fetch is str, it needs to be change into list for VCF records to process
    response = [
    [
            v.CHROM,
            str(v.POS),  # still 1-based
            v.ID or ".",
            v.REF,
            ",".join(v.ALT),
            str(v.QUAL) if v.QUAL is not None else ".",
            v.FILTER or ".",
        ]
        for v in result
    ]
    #######YUE################
    return [VCFRecord(line) for line in response]

def get_vcf_records_0base(tbx, chrom, pos):
    """ Uses tabix to query VCF file. Parses info from record.
    Args:
        in_vcf (str): vcf file
        chrom (str): chromosome
        pos (int): base pair position
    Returns:
        list of VCFRecords
    """
    region = f"{chrom}:{int(pos)+1}-{int(pos)+2}"  # input is 0-based
    result = tbx(region)
    # each records returned by fetch is str, it needs to be change into list for VCF records to process
    response = [
    [
            v.CHROM,
            str(v.POS),  # still 1-based
            v.ID or ".",
            v.REF,
            ",".join(v.ALT),
            str(v.QUAL) if v.QUAL is not None else ".",
            v.FILTER or ".",
        ]
        for v in result
    ]
    return [VCFRecord(line) for line in response]